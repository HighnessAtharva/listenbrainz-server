import json
from datetime import timedelta, datetime
from pathlib import Path

from more_itertools import chunked
from pyspark import Row

import listenbrainz_spark
from listenbrainz_spark.constants import LAST_FM_FOUNDING_YEAR
from listenbrainz_spark.schema import recent_releases_schema
from listenbrainz_spark.stats import run_query
from listenbrainz_spark.utils import get_latest_listen_ts, get_listens_from_new_dump

USERS_PER_MESSAGE = 5


def load_all_releases():
    with Path(__file__).parent.joinpath("release.json").open() as f:
        data = json.load(f)

        releases = []
        for release in data:
            releases.append(Row(
                date=release["date"],
                artist_credit_name=release["artist_credit_name"],
                artist_mbids=release["artist_mbids"],
                release_name=release["release_name"],
                release_mbid=release["release_mbid"],
                release_group_primary_type=release["release_group_primary_type"],
                release_group_secondary_type=release["release_group_secondary_type"]
            ))

        return listenbrainz_spark.session.createDataFrame(
            releases,
            schema=recent_releases_schema
        )


def get_query():
    return """
        WITH artists AS (
            SELECT DISTINCT explode(artist_mbids) AS artist_mbid
              FROM recent_releases
        ), exploded_listens AS (
            SELECT user_id
                 , explode(artist_credit_mbids) AS artist_mbid
              FROM recent_listens
        ), artist_discovery AS (
            SELECT user_id
                 , artist_mbid
                 , count(*) AS partial_confidence
              FROM exploded_listens
              JOIN artists
             USING (artist_mbid)
          GROUP BY user_id, artist_mbid
        ), filtered_releases AS (
            SELECT ad.user_id
                 , rr.release_name
                 , rr.release_mbid
                 , rr.artist_credit_name
                 , rr.artist_mbids
                 , rr.date
                 , rr.release_group_primary_type
                 , rr.release_group_secondary_type
                 , SUM(partial_confidence) AS confidence
              FROM artist_discovery ad
              JOIN recent_releases rr
                ON array_contains(rr.artist_mbids, ad.artist_mbid)
          GROUP BY ad.user_id
                 , rr.release_name
                 , rr.release_mbid
                 , rr.artist_credit_name
                 , rr.artist_mbids
                 , rr.date
                 , rr.release_group_primary_type
                 , rr.release_group_secondary_type
        )
        SELECT user_id
             , array_sort(
                    collect_list(
                        struct(
                            release_name
                          , release_mbid
                          , artist_credit_name
                          , artist_mbids
                          , date
                          , release_group_primary_type
                          , release_group_secondary_type
                          , confidence
                        )
                    )
                   , (left, right) -> CASE
                                      WHEN left.confidence > right.confidence THEN -1
                                      WHEN left.confidence < right.confidence THEN  1
                                      ELSE 0
                                      END
                    -- sort in descending order of confidence              
               ) AS releases
          FROM filtered_releases
      GROUP BY user_id      
    """


def main(days: int, database: str):
    to_date = get_latest_listen_ts()
    if days:
        from_date = to_date + timedelta(days=-days)
    else:
        from_date = datetime(LAST_FM_FOUNDING_YEAR, 1, 1)
    get_listens_from_new_dump(from_date, to_date) \
        .createOrReplaceTempView("recent_listens")

    load_all_releases().createOrReplaceTempView("recent_releases")

    itr = run_query(get_query()).toLocalIterator()
    for rows in chunked(itr, USERS_PER_MESSAGE):
        yield {
            "type": "recent_releases",
            "database": database,
            "data": [row.asDict(recursive=True) for row in rows]
        }