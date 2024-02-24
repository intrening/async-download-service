import argparse
import logging
from aiohttp import web
import aiofiles
import os
from dotenv import load_dotenv
import asyncio
from functools import partial

load_dotenv()

ZIP_FILE_NAME = "photo_archive.zip"
CHUNK_SIZE_BYTES = 102400  # 100Кб в байтах


async def archive(request, photo_files_path='', sleep_time=None):
    archive_hash = request.match_info.get('archive_hash', None)
    archive_directory = os.path.join(photo_files_path, archive_hash)
    if not os.path.exists(archive_directory):
        raise web.HTTPNotFound(text="Архив не существует или был удален")

    response = web.StreamResponse()
    response.headers['Content-Disposition'] = f'attachment; filename={ZIP_FILE_NAME}'
    await response.prepare(request)
    zip_process = await asyncio.create_subprocess_exec(
        "zip", "-r", "-", ".",
        cwd=archive_directory,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        while True:
            stdout = await zip_process.stdout.read(CHUNK_SIZE_BYTES)
            if not stdout:
                logging.debug("Finish sending archive chunk")
                break
            logging.debug("Sending archive chunk ...")
            await response.write(stdout)
            if sleep_time:
                logging.debug(f"Sleep for {sleep_time} sec")
                await asyncio.sleep(sleep_time)
    except asyncio.CancelledError:
        logging.debug("Download was interrupted")
        raise
    except ConnectionResetError:
        logging.debug("ConnectionResetError: Connection was reset")
        raise
    except SystemExit:
        logging.debug("SystemExit: System exit signal received")
        raise
    finally:
        await zip_process.communicate()
        if zip_process.returncode is None:
            zip_process.kill()
            await zip_process.wait()


async def handle_index_page(request):
    async with aiofiles.open('index.html', mode='r') as index_file:
        index_contents = await index_file.read()
    return web.Response(text=index_contents, content_type='text/html')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Server for photo archive',
    )
    parser.add_argument(
        '--sleep', '-s', type=float, nargs='?', const=1.0,
        help='sleep time in sec between sending chunks (default: no sleep)(const: 1.0 sec)'
    )
    parser.add_argument('--debug', help='debug mode', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.ERROR)
    if args.sleep:
        logging.debug(f"Set sleep time: {args.sleep} sec")
    photo_files_path = os.getenv('PHOTO_FILES_PATH', './photos')
    logging.debug(f"Photo files path: {photo_files_path}")

    logging.debug("Start server")
    app = web.Application()
    app.add_routes([
        web.get('/', handle_index_page),
        web.get('/archive/{archive_hash}/', partial(
            archive,
            photo_files_path=photo_files_path,
            sleep_time=args.sleep)
        ),
    ])
    web.run_app(app)
