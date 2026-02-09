import asyncio
import os
import aiohttp
import ee
import geemap
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import time

#  авторизация в GEE
# ee.Authenticate()
# ee.Initialize(project = 'iwp-dev-383806')


async def download_cmip6_image(image: ee.Image, download_path: str, geom, scale: int = 25000) -> bool:
    """
    Download a single CMIP6 image asynchronously.

    :param image: Earth Engine Image object
    :param download_path: Path to save the downloaded file
    :param geom: Geometry region for export
    :param scale: Scale/resolution in meters
    :return: True if download successful, False otherwise
    """
    try:
        # Get image properties
        properties = image.getInfo()['properties']
        date = image.date().format('YYYY-MM-dd').getInfo()

        # Create download URL with proper parameters for CMIP6
        url = image.getDownloadURL({
            'region': geom,
            'scale': scale,
            'format': 'GeoTIFF',
            'crs': 'EPSG:4326',
            'bands': image.bandNames().getInfo()[0]  # Get first band name
        })

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(download_path), exist_ok=True)

        # Download with retry logic
        max_retries = 3
        for retry in range(max_retries):
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            with open(download_path, 'wb') as f:
                                content = await response.read()
                                f.write(content)

                            # Verify file was written
                            if os.path.exists(download_path) and os.path.getsize(download_path) > 0:
                                print(f"✓ Downloaded: {os.path.basename(download_path)} ({len(content):,} bytes)")
                                return True
                            else:
                                print(f"✗ File empty or not created: {download_path}")
                        else:
                            print(f"✗ Download failed: {response.status} for {download_path}")

                # Wait before retry
                if retry < max_retries - 1:
                    await asyncio.sleep(2 ** retry)  # Exponential backoff

            except Exception as e:
                if retry < max_retries - 1:
                    print(f"Retry {retry + 1}/{max_retries} for {download_path}: {str(e)}")
                    await asyncio.sleep(2 ** retry)
                else:
                    print(f"✗ Failed after {max_retries} retries: {download_path}, Error: {str(e)}")

        return False

    except Exception as e:
        print(f"✗ Error processing image: {str(e)}")
        return False


async def export_cmip6_collection_parallel(coll, path: str, geom,
                                           max_concurrent: int = 3,
                                           batch_size: int = 10) -> Dict[str, int]:
    """
    Export CMIP6 image collection in parallel.

    :param coll: Earth Engine ImageCollection
    :param path: Directory path for downloads
    :param geom: Geometry region for export
    :param max_concurrent: Maximum concurrent downloads
    :param batch_size: Number of images to process in each batch
    :return: Dictionary with download statistics
    """
    stats = {'total': 0, 'successful': 0, 'failed': 0}

    try:
        # Get image collection info
        coll_info = coll.getInfo()
        images_info = coll_info['features']
        stats['total'] = len(images_info)

        print(f"Found {stats['total']} images to download")

        if stats['total'] == 0:
            print("No images found in collection")
            return stats

        # Process images in batches to avoid memory issues
        for batch_start in range(0, stats['total'], batch_size):
            batch_end = min(batch_start + batch_size, stats['total'])
            batch_images = images_info[batch_start:batch_end]

            print(
                f"\nProcessing batch {batch_start // batch_size + 1}/{(stats['total'] + batch_size - 1) // batch_size}")

            # Prepare download tasks for this batch
            tasks = []
            for i, img_info in enumerate(batch_images):
                # Create image from ID
                image_id = img_info['id']
                image = ee.Image(image_id)

                # Get date and other properties
                properties = img_info.get('properties', {})

                # Extract date from properties
                date_str = None
                if 'system:time_start' in properties:
                    timestamp = properties['system:time_start']
                    date_str = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
                elif 'system:index' in properties:
                    # Try to extract date from image ID
                    parts = image_id.split('_')
                    for part in parts:
                        if len(part) == 8 and part.isdigit():
                            try:
                                date_str = datetime.strptime(part, '%Y%m%d').strftime('%Y-%m-%d')
                                break
                            except:
                                continue

                if not date_str:
                    date_str = f"image_{batch_start + i}"

                # Create filename
                # Extract model/scenario from image ID
                model_scenario = "CMIP6"
                if 'model' in properties:
                    model_scenario = properties.get('model', 'CMIP6')
                elif 'scenario' in properties:
                    model_scenario = f"{model_scenario}_{properties.get('scenario', '')}"

                filename = f"{model_scenario}_{date_str}.tif"
                filename = filename.replace('/', '_').replace(':', '_')  # Sanitize filename
                download_path = os.path.join(path, filename)

                # Skip if file already exists
                if os.path.exists(download_path):
                    file_size = os.path.getsize(download_path)
                    if file_size > 1024:  # File exists and has reasonable size
                        print(f"✓ Skipping (exists): {filename} ({file_size:,} bytes)")
                        stats['successful'] += 1
                        continue
                    else:
                        print(f"⚠ File exists but seems small, redownloading: {filename}")

                # Create download task
                tasks.append(download_cmip6_image(image, download_path, geom))

            # Execute tasks with concurrency limit
            semaphore = asyncio.Semaphore(max_concurrent)

            async def download_with_semaphore(task):
                async with semaphore:
                    return await task

            # Run all tasks for this batch
            batch_tasks = [download_with_semaphore(task) for task in tasks]
            if batch_tasks:
                results = await asyncio.gather(*batch_tasks, return_exceptions=True)

                # Count results
                for result in results:
                    if isinstance(result, Exception):
                        print(f"✗ Task failed with exception: {str(result)}")
                        stats['failed'] += 1
                    elif result:
                        stats['successful'] += 1
                    else:
                        stats['failed'] += 1

            # Small delay between batches
            if batch_end < stats['total']:
                await asyncio.sleep(1)

        return stats

    except Exception as e:
        print(f"✗ Error in export_cmip6_collection_parallel: {str(e)}")
        return stats


def getCMIP6_async(dateStart, dateEnd, geom, collection: Optional[str] = None,
                   home: str = '.', max_concurrent: int = 3,
                   variables: Optional[List[str]] = None) -> Dict[str, Dict[str, int]]:
    """
    Download CMIP6 data asynchronously with proper CMIP6 handling.

    :param dateStart: Start date (YYYY-MM-DD)
    :param dateEnd: End date (YYYY-MM-DD)
    :param geom: Geometry for region of interest
    :param collection: CMIP6 collection name
    :param home: Base directory for downloads
    :param max_concurrent: Maximum concurrent downloads per variable
    :param variables: List of variables to download (None for all)
    :return: Dictionary with statistics per variable
    """

    if not collection:
        collection = 'NASA/GDDP-CMIP6'

    # CMIP6 bands mapping
    cmip6_bands = {
        'tas': 'Near-Surface Air Temperature',
        'tasmin': 'Daily Minimum Near-Surface Air Temperature',
        'tasmax': 'Daily Maximum Near-Surface Air Temperature',
        'pr': 'Precipitation',
        'sfcWind': 'Near-Surface Wind Speed'
    }

    # Filter bands if specific variables requested
    if variables:
        filtered_bands = {}
        for var in variables:
            if var in cmip6_bands:
                filtered_bands[var] = cmip6_bands[var]
            else:
                print(f"Warning: Variable '{var}' not recognized. Available: {list(cmip6_bands.keys())}")
        if filtered_bands:
            cmip6_bands = filtered_bands
        else:
            print("No valid variables specified, downloading all available")

    print(f"Requesting CMIP6 data from {collection}")
    print(f"Time period: {dateStart} to {dateEnd}")
    print(f"Variables to download: {list(cmip6_bands.keys())}")
    print(f"Download directory: {home}")
    print("-" * 50)

    # Create main download directory
    os.makedirs(home, exist_ok=True)

    # Statistics tracker
    all_stats = {}

    # Process each variable sequentially but download images in parallel
    for var_code, var_name in cmip6_bands.items():
        print(f"\n{'=' * 60}")
        print(f"Processing: {var_name} ({var_code})")
        print(f"{'=' * 60}")

        try:
            # Create image collection for this variable
            coll = (ee.ImageCollection(collection)
                    .filterBounds(geom)
                    .filterDate(dateStart, dateEnd)
                    .select(var_code))

            # Get collection size
            count = coll.size().getInfo()
            print(f"Found {count} images")

            if count == 0:
                print(f"No images found for {var_code}, skipping...")
                all_stats[var_code] = {'total': 0, 'successful': 0, 'failed': 0}
                continue

            # Create variable directory
            var_dir = os.path.join(home, var_code)
            os.makedirs(var_dir, exist_ok=True)

            # Get some metadata about the collection
            try:
                first_img = ee.Image(coll.first())
                first_info = first_img.getInfo()
                print(f"Data type: {first_info.get('type', 'Unknown')}")
                print(f"Bands: {first_img.bandNames().getInfo()}")
            except:
                print("Could not retrieve detailed metadata")

            # Run async download
            start_time = time.time()

            # Create event loop for this variable
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                stats = loop.run_until_complete(
                    export_cmip6_collection_parallel(coll, var_dir, geom, max_concurrent)
                )
                all_stats[var_code] = stats

                elapsed_time = time.time() - start_time
                print(f"\nCompleted {var_code} in {elapsed_time:.1f} seconds")
                print(f"Successfully downloaded: {stats['successful']}/{stats['total']}")
                if stats['failed'] > 0:
                    print(f"Failed downloads: {stats['failed']}")

            except Exception as e:
                print(f"✗ Error downloading {var_code}: {str(e)}")
                all_stats[var_code] = {'total': count, 'successful': 0, 'failed': count}
            finally:
                loop.close()

        except Exception as e:
            print(f"✗ Failed to process {var_code}: {str(e)}")
            all_stats[var_code] = {'total': 0, 'successful': 0, 'failed': 0}

        # Small delay between variables
        time.sleep(2)

    # Print summary
    print(f"\n{'=' * 60}")
    print("DOWNLOAD SUMMARY")
    print(f"{'=' * 60}")
    total_successful = 0
    total_failed = 0
    total_files = 0

    for var_code, stats in all_stats.items():
        print(f"{var_code}: {stats['successful']}/{stats['total']} successful")
        total_successful += stats['successful']
        total_failed += stats['failed']
        total_files += stats['total']

    print(f"\nOverall: {total_successful}/{total_files} files downloaded successfully")
    if total_failed > 0:
        print(f"Failed: {total_failed} files")

    return all_stats


# Alternative simplified version using geemap's built-in functionality
def getCMIP6_simple(dateStart, dateEnd, geom, collection: Optional[str] = None,
                    home: str = '.', variables: Optional[List[str]] = None) -> None:
    """
    Simplified version using geemap's export functionality with progress tracking.
    """

    if not collection:
        collection = 'NASA/GDDP-CMIP6'

    bands = {
        'tas': 'temp',
        'tasmin': 'tempmin',
        'tasmax': 'tempmax',
        'pr': 'prec',
        'sfcWind': 'wind'
    }

    # Filter if specific variables requested
    if variables:
        filtered_bands = {}
        for var in variables:
            for ee_name, dir_name in bands.items():
                if var in [ee_name, dir_name]:
                    filtered_bands[dir_name] = ee_name
        if filtered_bands:
            bands = {v: k for k, v in filtered_bands.items()}

    print(f'Requesting data {collection} for region {geom} from {dateStart} to {dateEnd}')

    for var_dir, band in bands.items():
        print(f"\nProcessing: {var_dir} ({band})")

        # Create image collection
        coll = (ee.ImageCollection(collection)
                .filterBounds(geom)
                .filterDate(dateStart, dateEnd)
                .select(band))

        count = coll.size().getInfo()
        print(f"Found {count} images")

        if count == 0:
            print("Skipping...")
            continue

        # Create output directory
        path = os.path.join(home, var_dir)
        os.makedirs(path, exist_ok=True)

        # Use geemap export (slower but reliable)
        try:
            print("Starting download (this may take a while)...")
            geemap.ee_export_image_collection(coll, path, region=geom)
            print(f"✓ Completed: {var_dir}")
        except Exception as e:
            print(f"✗ Error downloading {var_dir}: {str(e)}")


def setGeom(*coords):

    # границы по пространству
    if not coords:
        minLon = 31
        maxLon = 37
        minLat = 44
        maxLat = 46  # задаём охват нужных данных - по умолчанию на Байкал
    else:
        minLon = coords[0]
        maxLon = coords[1]
        minLat = coords[2]
        maxLat = coords[3]
    geom = ee.Geometry.Polygon([[[minLon, minLat],
                                 [minLon, maxLat],
                                 [maxLon, maxLat],
                                 [maxLon, minLat],
                                 [minLon, minLat]]])  # создаём геометрию области интересов
    feature = ee.Feature(geom, {})  # создаём пространственный объект без атрибутов
    geom = feature.geometry()
    # map = geemap.Map()
    # map.centerObject(geom, zoom = 5) # Задаём местоположение и масштаб
    # map.addLayer(geom)
    # map # Извлекаем геометрию, добавляем слой на карту
    return geom


# Example usage
if __name__ == "__main__":



    # Initialize Earth Engine
    try:
        #  авторизация в GEE
        ee.Authenticate()
        ee.Initialize(project='iwp-dev-383806')
        print("Earth Engine initialized successfully")
    except Exception as e:
        print(f"Failed to initialize Earth Engine: {str(e)}")
        print("Please authenticate first: ee.Authenticate()")
        exit(1)

    # Define parameters - use smaller region for testing
    # Define parameters
    dateStart = '2020-01-01'
    dateEnd = '2020-01-31'
    geom = setGeom()
    home = 'd:/EcoMeteo/CMIP6/krym'


    # Method 1: Async version (recommended for large downloads)
    print("\n" + "=" * 60)
    print("Starting async CMIP6 download")
    print("=" * 60)

    try:
        stats = getCMIP6_async(
            dateStart=dateStart,
            dateEnd=dateEnd,
            geom=geom,
            collection='NASA/GDDP-CMIP6',
            home=home,
            max_concurrent=2,  # Start with 2 concurrent downloads
            variables=['tas', 'pr', 'tasmin', 'tasmax', 'sfcWind']  # Download only temperature and precipitation for testing
        )
        print("\nDownload completed!")
    except KeyboardInterrupt:
        print("\nDownload interrupted by user")
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")

    # Method 2: Simple version (fallback)
    # getCMIP6_simple(dateStart, dateEnd, geom, home=home, variables=['temp', 'prec'])