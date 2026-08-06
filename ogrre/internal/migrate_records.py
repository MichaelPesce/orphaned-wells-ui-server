import sys
import os
import logging

# Set up logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
_log = logging.getLogger(__name__)

# Ensure package is in path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


from dotenv import load_dotenv
# Load environment from root or ogrre directory
load_dotenv()
load_dotenv(dotenv_path=os.path.join("ogrre", ".env"))

try:
    from ogrre.internal.data_manager import DataManager
except ImportError as e:
    _log.error(f"Failed to import DataManager. Make sure to run this script from the project root. Error: {e}")
    sys.exit(1)

def run_migration():
    _log.info("Starting record sequence number and counter migration...")
    try:
        manager = DataManager()
        # Explicit call (also triggered via DataManager.__init__)
        manager.migrateExistingRecords()
        _log.info("Migration finished successfully.")
    except Exception as e:
        _log.error(f"Migration script failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_migration()
