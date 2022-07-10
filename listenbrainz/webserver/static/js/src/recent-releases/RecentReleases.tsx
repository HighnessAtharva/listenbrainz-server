import * as React from "react";
import * as ReactDOM from "react-dom";
import * as Sentry from "@sentry/react";
import { Integrations } from "@sentry/tracing";
import { isEqual, uniqBy, uniqWith } from "lodash";
import { withAlertNotifications } from "../notifications/AlertNotificationsHOC";
import APIServiceClass from "../utils/APIService";
import GlobalAppContext, { GlobalAppContextT } from "../utils/GlobalAppContext";

import { getPageProps } from "../utils/utils";
import ErrorBoundary from "../utils/ErrorBoundary";
import ReleaseCard from "./ReleaseCard";
import * as fakeData from "./fakeData.json";
import ReleaseFilters from "./ReleaseFilters";

export default function RecentReleases() {
  const RELEASE_TYPE_OTHER = "Other";

  const [typesList, setTypesList] = React.useState<Array<string | null>>([]);

  React.useEffect(() => {
    setTypesList(
      uniqWith(
        fakeData.map((release) => {
          if (
            release.release_group_primary_type !== null ||
            release.release_group_secondary_type !== null
          )
            return (
              release.release_group_primary_type ||
              release.release_group_secondary_type
            );

          return RELEASE_TYPE_OTHER;
        }),
        isEqual
      )
    );
  }, []);

  return (
    <>
      <h3 id="row">Recent and upcoming releases</h3>
      <div className="releases-page row">
        <div
          className="col-md-1 hidden-xs hidden-sm hidden-md"
          style={{ padding: "2rem 0" }}
        >
          <ReleaseFilters filters={typesList} />
        </div>
        <div className="release-cards-grid col-xs-12 col-md-10">
          {// Deduplicate releases based on same release name by same artist name.
          uniqBy(fakeData, (datum) => {
            return (
              /*
               * toLowerCase() solves an edge case.
               * Example:
               * "release_name": "Waterslide, Diving Board, Ladder to the Sky"
               * "release_name": "Waterslide, Diving Board, Ladder To The Sky"
               * These releases will be considered unique.
               */
              datum.release_name.toLowerCase() +
              datum.artist_credit_name.toLowerCase()
            );
          })
            .slice(990, 1005)
            .map((release) => {
              return (
                <ReleaseCard
                  key={release.release_mbid}
                  releaseDate={release.date}
                  releaseMBID={release.release_mbid}
                  releaseName={release.release_name}
                  releaseType={
                    release.release_group_primary_type ||
                    release.release_group_secondary_type
                  }
                  artistCreditName={release.artist_credit_name}
                  artistMBIDs={release.artist_mbids}
                />
              );
            })}
        </div>
        <div className="releases-timeline col-xs-12 col-md-1">Timeline</div>
      </div>
    </>
  );
}

document.addEventListener("DOMContentLoaded", () => {
  const {
    domContainer,
    // reactProps,
    globalReactProps,
    // optionalAlerts,
  } = getPageProps();
  const {
    api_url,
    sentry_dsn,
    current_user,
    spotify,
    youtube,
    sentry_traces_sample_rate,
  } = globalReactProps;
  // const {} = reactProps;

  if (sentry_dsn) {
    Sentry.init({
      dsn: sentry_dsn,
      integrations: [new Integrations.BrowserTracing()],
      tracesSampleRate: sentry_traces_sample_rate,
    });
  }

  const RecentReleasesPageWithAlertNotifications = withAlertNotifications(
    RecentReleases
  );

  const apiService = new APIServiceClass(
    api_url || `${window.location.origin}/1`
  );

  const globalProps: GlobalAppContextT = {
    APIService: apiService,
    currentUser: current_user,
    spotifyAuth: spotify,
    youtubeAuth: youtube,
  };

  ReactDOM.render(
    <ErrorBoundary>
      <GlobalAppContext.Provider value={globalProps}>
        <RecentReleasesPageWithAlertNotifications />
      </GlobalAppContext.Provider>
    </ErrorBoundary>,
    domContainer
  );
});
