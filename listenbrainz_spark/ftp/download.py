import os
import re
import tempfile
import time
import logging
from typing import List

from listenbrainz_spark import config
from listenbrainz_spark.ftp import ListenBrainzFTPDownloader, DumpType, ListensDump
from listenbrainz_spark.exceptions import DumpNotFoundException

# mbid_msid_mapping_with_matchable is used.
# refer to: http://ftp.musicbrainz.org/pub/musicbrainz/listenbrainz/labs/mappings/
ARTIST_RELATION_DUMP_ID_POS = 5

FULL = 'full'
INCREMENTAL = 'incremental'

logger = logging.getLogger(__name__)


class ListenbrainzDataDownloader(ListenBrainzFTPDownloader):

    def get_dump_name_to_download(self, dump, dump_id, dump_id_pos):
        """ Get name of the dump to be downloaded.

            Args:
                dump (list): Contents of the directory from which dump will be downloaded.
                dump_id (int): Unique indentifier of dump to be downloaded .
                dump_id_pos (int): Unique identifier position in dump name.

            Returns:
                req_dump (str): Name of the dump to be downloaded.
        """
        if dump_id:
            req_dump = next(
                (
                    dump_name
                    for dump_name in dump
                    if int(dump_name.split('-')[dump_id_pos]) == dump_id
                ),
                None,
            )
            if req_dump is None:
                err_msg = f"Could not find dump with ID: {dump_id}. Aborting..."
                raise DumpNotFoundException(err_msg)
        else:
            req_dump = dump[-1]
        return req_dump

    def get_dump_archive_name(self, dump_name):
        """ Get the name of the Spark dump archive from the dump directory name.

            Args:
                dump_name (str): FTP dump directory name.

            Returns:
                '' : Spark dump archive name.
        """
        return f'{dump_name}.tar.bz2'

    def get_listens_dump_file_name(self, dump_name):
        """ Get the name of Spark listens dump name archive.

            Returns:
                str : Spark listens dump archive name.
        """
        return ListensDump.from_ftp_dir(dump_name).get_dump_file()

    def get_available_dumps(self, dump, mapping_name_prefix):
        """ Get list of available mapping dumps.

            Args:
                dump: list of dumps in the current working directory.
                mapping_name_prefix (str): prefix of mapping dump name.

            Returns:
                mapping: list of mapping dump names in the current working directory.
        """
        mapping = []
        for mapping_name in dump:
            mapping_pattern = f'{mapping_name_prefix}-\\d+-\\d+(.tar.bz2)$'

            if re.match(mapping_pattern, mapping_name):
                mapping.append(mapping_name)

        if not mapping:
            err_msg = f'{mapping_name_prefix} type mapping not found'
            raise DumpNotFoundException(err_msg)

        return mapping

    def download_listens(self, directory, listens_dump_id=None, dump_type: DumpType = DumpType.FULL):
        """ Download listens to dir passed as an argument.

            Args:
                directory (str): Dir to save listens locally.
                listens_dump_id (int): Unique identifier of listens to be downloaded.
                    If not provided, most recent listens will be downloaded.
                dump_type: type of dump, full or incremental

            Returns:
                dest_path (str): Local path where listens have been downloaded.
                listens_file_name (str): name of downloaded listens dump.
                dump_id (int): Unique indentifier of downloaded listens dump.
        """
        if dump_type == DumpType.INCREMENTAL:
            ftp_cwd = os.path.join(config.FTP_LISTENS_DIR, 'incremental/')
        else:
            ftp_cwd = os.path.join(config.FTP_LISTENS_DIR, 'fullexport/')
        self.connection.cwd(ftp_cwd)
        listens_dump_list = sorted(self.list_dir(), key=lambda x: int(x.split('-')[2]))
        req_listens_dump = self.get_dump_name_to_download(listens_dump_list, listens_dump_id, 2)
        dump_id = req_listens_dump.split('-')[2]

        self.connection.cwd(req_listens_dump)
        listens_file_name = self.get_listens_dump_file_name(req_listens_dump)

        t0 = time.monotonic()
        logger.info(f'Downloading {listens_file_name} from FTP...')
        dest_path = self.download_dump(listens_file_name, directory)
        logger.info('Done. Total time: {:.2f} sec'.format(time.monotonic() - t0))
        return dest_path, listens_file_name, int(dump_id)

    def download_artist_relation(self, directory, artist_relation_dump_id=None):
        """ Download artist relation to dir passed as an argument.

            Args:
                directory (str): Dir to save artist relation locally.
                artist_relation_dump_id (int): Unique identifier of artist relation to be downloaded.
                    If not provided, most recent artist relation will be downloaded.

            Returns:
                dest_path (str): Local path where artist relation has been downloaded.
                artist_relation_file_name (str): file name of downloaded artist relation.
        """
        self.connection.cwd(config.FTP_ARTIST_RELATION_DIR)
        dump = self.list_dir()
        req_dump = self.get_dump_name_to_download(dump, artist_relation_dump_id, ARTIST_RELATION_DUMP_ID_POS)

        self.connection.cwd(req_dump)
        artist_relation_file_name = self.get_dump_archive_name(req_dump)

        t0 = time.monotonic()
        logger.info(f'Downloading {artist_relation_file_name} from FTP...')
        dest_path = self.download_dump(artist_relation_file_name, directory)
        logger.info('Done. Total time: {:.2f} sec'.format(time.monotonic() - t0))

        return dest_path, artist_relation_file_name

    def download_release_json_dump(self, directory):
        self.connection.cwd(config.FTP_MUSICBRAINZ_DIR)
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file = os.path.join(temp_dir, "LATEST")
            self.download_file_binary("LATEST", temp_file)
            with open(temp_file) as f:
                dump_name = f.readline().strip()
        self.connection.cwd(dump_name)

        logger.info(f"Downloading release.tar.gz of dump {dump_name} from FTP...")
        t0 = time.monotonic()
        filename = "release.tar.xz"
        dest = os.path.join(directory, filename)
        self.download_file_binary(filename, dest)
        logger.info(f"Done. Total time: {time.monotonic() - t0:.2f} sec")
        return dest

    def get_latest_dump_id(self, dump_type: DumpType):
        if dump_type == DumpType.INCREMENTAL:
            ftp_cwd = os.path.join(config.FTP_LISTENS_DIR, 'incremental/')
        else:
            ftp_cwd = os.path.join(config.FTP_LISTENS_DIR, 'fullexport/')
        self.connection.cwd(ftp_cwd)

        listens_dumps = [ListensDump.from_ftp_dir(name) for name in self.list_dir()]
        listens_dumps.sort(key=lambda x: x.dump_id)
        return listens_dumps[-1].dump_id
