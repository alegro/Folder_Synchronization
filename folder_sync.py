import os
import shutil
import time
import hashlib
import logging
import argparse
from pathlib import Path


def setup_logging(log_file_path):
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file_path), logging.StreamHandler()],
    )


def calculate_md5(file_path):
    """Calculate the MD5 hash of a file to check for content changes."""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except IOError as e:
        logging.error(f"Error calculating MD5 for {file_path}: {e}")
        return None


def sync_directories(source_folder, replica_folder):
    """Synchronize the replica folder with the source folder."""
    source_path = Path(source_folder)
    replica_path = Path(replica_folder)

    # Ensure the replica folder exists
    if not replica_path.exists():
        replica_path.mkdir(parents=True)
        logging.info(f"Created replica directory: {replica_folder}")

    # Sync files and directories from source to replica
    for item in source_path.rglob("*"):
        relative_path = item.relative_to(source_path)
        replica_item = replica_path / relative_path

        if item.is_dir():
            # Ensure directory exists in the replica
            if not replica_item.exists():
                replica_item.mkdir(parents=True)
                logging.info(f"Created directory: {replica_item}")
        else:
            # Sync file from source to replica
            if not replica_item.exists():
                shutil.copy2(item, replica_item)
                logging.info(f"Copied file: {replica_item}")
            else:
                # Compare MD5 checksums to decide whether to replace the file
                if item.stat().st_size == replica_item.stat().st_size:
                    source_md5 = calculate_md5(item)
                    replica_md5 = calculate_md5(replica_item)
                    if source_md5 != replica_md5:
                        shutil.copy2(item, replica_item)
                        logging.info(f"Updated file: {replica_item}")
                else:
                    shutil.copy2(item, replica_item)
                    logging.info(f"Updated file due to size change: {replica_item}")

    # Remove files and directories in the replica not present in the source
    for item in replica_path.rglob("*"):
        relative_path = item.relative_to(replica_path)
        source_item = source_path / relative_path

        if not source_item.exists():
            if item.is_dir():
                shutil.rmtree(item)
                logging.info(f"Deleted directory: {item}")
            else:
                item.unlink()
                logging.info(f"Deleted file: {item}")


def main(source_folder, replica_folder, sync_interval, log_file_path):
    """Main function to manage folder synchronization and periodic execution."""
    setup_logging(log_file_path)

    logging.info(
        f"Starting folder synchronization: {source_folder} -> {replica_folder}"
    )
    logging.info(f"Synchronization interval: {sync_interval} seconds")

    try:
        while True:
            sync_directories(source_folder, replica_folder)
            logging.info("Synchronization complete.")
            time.sleep(sync_interval)
    except KeyboardInterrupt:
        logging.info("Synchronization interrupted by user.")
    except Exception as e:
        logging.error(f"An error occurred during synchronization: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="One-way folder synchronization tool.")
    parser.add_argument("source_folder", type=str, help="Path to the source folder.")
    parser.add_argument("replica_folder", type=str, help="Path to the replica folder.")
    parser.add_argument(
        "sync_interval", type=int, help="Synchronization interval in seconds."
    )
    parser.add_argument("log_file_path", type=str, help="Path to the log file.")

    args = parser.parse_args()

    # Check that both source and replica directories are valid
    if not os.path.isdir(args.source_folder):
        raise ValueError(
            f"Source folder does not exist or is not a directory: {args.source_folder}"
        )
    if not os.path.isdir(args.replica_folder):
        logging.warning(
            f"Replica folder does not exist. It will be created at: {args.replica_folder}"
        )

    main(
        args.source_folder, args.replica_folder, args.sync_interval, args.log_file_path
    )

# Running the following command in the terminal will start folder synchronization:
# python folder_sync.py /path/to/source /path/to/replica 60 /path/to/logfile.log
# The synchronization occurs every 60 seconds (modifiable by changing the interval),
# and the file paths should be updated to your desired directories.
