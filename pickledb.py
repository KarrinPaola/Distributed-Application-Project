import datetime
import os
import orjson
from cryptography.fernet import Fernet

class PickleDB:
    def __init__(self, location, backup_dir="backups", encryption=False,):
        """
        Initialize the PickleDB object.

        Args:
            location (str): Path to the database file.
            backup_dir (str): Directory for storing backups.
            encryption (bool): Whether to enable encryption.
        """
        self.backup_dir = os.path.expanduser(backup_dir)
        os.makedirs(self.backup_dir, exist_ok=True)
        self.location = os.path.expanduser(location)
        self.encryption = encryption
        self.key_file = f"{self.location}.key"
        self.key = None
        
        if self.encryption:
            self._setup_encryption()
        
        self._load()
    
    def _setup_encryption(self):
        """
        Setup encryption by generating or loading an encryption key.
        """
        if os.path.exists(self.key_file):
            with open(self.key_file, 'rb') as f:
                self.key = f.read()
        else:
            self.key = Fernet.generate_key()
            with open(self.key_file, 'wb') as f:
                f.write(self.key)
        self.cipher = Fernet(self.key)
    
    def _encrypt(self, data):
        """
        Encrypt data if encryption is enabled.
        """
        return self.cipher.encrypt(data.encode()) if self.encryption else data.encode()
    
    def _decrypt(self, data):
        """
        Decrypt data if encryption is enabled.
        """
        return self.cipher.decrypt(data).decode() if self.encryption else data.decode()
    
    def _create_backup(self):
        """
        Create a backup of the current database.
        """
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(self.backup_dir, f"backup_{timestamp}.json")
        try:
            with open(backup_path, 'wb') as backup_file:
                backup_file.write(self._encrypt(orjson.dumps(self.db).decode()))
        except Exception as e:
            print(f"Failed to create backup: {e}")
    
    def _load(self):
        """
        Load data from the JSON file if it exists, or initialize an empty database.
        """
        if os.path.exists(self.location) and os.path.getsize(self.location) > 0:
            try:
                with open(self.location, 'rb') as f:
                    data = f.read()
                    data = self._decrypt(data)
                    self.db = orjson.loads(data)
            except Exception as e:
                self.db = {}
                print(f"Failed to load database: {e}")
        else:
            self.db = {}
    
    def save(self, backup=True):
        """
        Save the database to disk, with optional backup creation.
        """
        temp_location = f"{self.location}.tmp"
        try:
            with open(temp_location, 'wb') as temp_file:
                temp_file.write(self._encrypt(orjson.dumps(self.db).decode()))
            os.replace(temp_location, self.location)
            if backup:
                self._create_backup()
            return True
        except Exception as e:
            print(f"Failed to write database to disk: {e}")
            return False
    
    def set(self, key, value):
        """
        Add or update a key-value pair in the database.
        """
        key = str(key)
        self.db[key] = value
        return True
    
    def get(self, key):
        """
        Retrieve the value associated with a key.
        """
        key = str(key)
        return self.db.get(key)
    
    def search_by_key(self, substring):
        return [key for key in self.db.keys() if substring in key]

    def search_by_value(self, search_value):
        return [key for key, value in self.db.items() if search_value in str(value)]

    def filter(self, condition):
        return {key: value for key, value in self.db.items() if condition(value)}

    from rapidfuzz import process

    def fuzzy_search(self, search_key, threshold=80):
        keys = list(self.db.keys())
        matches = process.extract(search_key, keys, limit=5)
        return [match[0] for match in matches if match[1] >= threshold]
    
    def remove(self, key):
        """
        Remove a key from the database.
        """
        key = str(key)
        if key in self.db:
            del self.db[key]
            return True
        return False
    
    def purge(self):
        """
        Clear all keys from the database.
        """
        self.db.clear()
        return True
    
    def all(self):
        """
        Retrieve a list of all keys in the database.
        """
        return list(self.db.keys())
    
    def list_backups(self):
        """
        List all available backup files.
        """
        return sorted(os.listdir(self.backup_dir))
    
    def restore(self, backup_filename):
        """
        Restore the database from a specified backup file.
        """
        backup_path = os.path.join(self.backup_dir, backup_filename)
        if not os.path.exists(backup_path):
            print(f"Backup file not found: {backup_filename}")
            return False
        try:
            with open(backup_path, 'rb') as backup_file:
                data = backup_file.read()
                self.db = orjson.loads(self._decrypt(data))
            self.save(backup=False)
            return True
        except Exception as e:
            print(f"Failed to restore backup: {e}")
            return False
    
    def cleanup_backups(self, max_backups=5):
        """
        Limit the number of backup files stored.
        """
        backups = sorted(os.listdir(self.backup_dir))
        if len(backups) > max_backups:
            for old_backup in backups[:-max_backups]:
                os.remove(os.path.join(self.backup_dir, old_backup))
