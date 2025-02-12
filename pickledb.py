import os
import orjson
import datetime
from cryptography.fernet import Fernet
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import logging

class PickleDB:
    def __init__(self, location, backup_dir="backups", encryption=False, log_file="db_activity.log"):
        """
        Initialize the PickleDB object.

        Args:
            location (str): Path to the database file.
            backup_dir (str): Directory for storing backups.
            encryption (bool): Whether to enable encryption.
            log_file (str): Path to the log file.
        """
        self.backup_dir = os.path.expanduser(backup_dir)
        os.makedirs(self.backup_dir, exist_ok=True)
        self.location = os.path.expanduser(location)
        self.encryption = encryption
        self.key_file = f"{self.location}.key"
        self.key = None
        self.log_file = log_file
        self.loaded = False  # Cờ kiểm soát trạng thái tải dữ liệu

        if self.encryption:
            self._setup_encryption()

        self._setup_logging()
        self._load()
        
    def _setup_logging(self):
        """Thiết lập logging để ghi lại lịch sử hoạt động."""
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    
    def _log(self, action, key, value=None):
        """Ghi log một hành động vào file log."""
        if value is not None:
            logging.info(f"{action} - Key: {key}, Value: {value}")
        else:
            logging.info(f"{action} - Key: {key}")
    
    def _setup_encryption(self):
        """
        Setup encryption by generating or loading an encryption key.
        """
        if os.path.exists(self.key_file):
            with open(self.key_file, 'rb') as f:
                self.key = f.read()
        else:
            # Kiểm tra nếu database đã tồn tại nhưng không có tệp khóa
            if os.path.exists(self.location):
                raise ValueError(
                    f"Encryption key file missing for existing database: {self.key_file}. "
                    "Please restore the key file or disable encryption."
                )
            
            # Tạo khóa mới nếu cả database và key file đều chưa tồn tại
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
                    
                    # Kiểm tra dữ liệu có mã hóa không
                    is_encrypted = data.startswith(b"gAAAA")
                    if is_encrypted and not self.encryption:
                        raise ValueError(
                            "Detected encrypted data, but encryption is disabled. "
                            "Enable encryption to load this database."
                        )
                    if not is_encrypted and self.encryption:
                        raise ValueError(
                            "Database is not encrypted, but encryption is enabled. "
                            "Disable encryption to load this database."
                        )

                    # Giải mã dữ liệu nếu cần
                    data = self._decrypt(data) if self.encryption else data.decode()
                    self.db = orjson.loads(data)
                    
                    self.loaded = True  # Đánh dấu là đã tải thành công

            except Exception as e:
                self.db = {}
                print(f"Failed to load database: {e}")
                self.loaded = False  # Chặn mọi thao tác khác
        else:
            self.db = {}
            self.loaded = True 
            
    def _check_loaded(self):
        """Kiểm tra xem database có tải thành công không trước khi thao tác."""
        if not self.loaded:
            print("⚠️ Cảnh báo: Cơ sở dữ liệu không tải thành công. Các thao tác sẽ không được thực hiện.")
            return False
        return True

    def save(self, backup=True, max_back_ups = 5):
        """
        Save the database to disk, with optional backup creation.
        """
        if not self._check_loaded():
            return   # ✅ Trả về danh sách rỗng thay vì lỗi
        temp_location = f"{self.location}.tmp"
        try:
            with open(temp_location, 'wb') as temp_file:
                temp_file.write(self._encrypt(orjson.dumps(self.db).decode()))
            os.replace(temp_location, self.location)
            if backup:
                self._create_backup()
                 # Kiểm tra số lượng backup và dọn dẹp nếu cần
                if len(self.list_backups()) > 5:
                    self.cleanup_backups(max_backups=max_back_ups)
            return True
        except Exception as e:
            print(f"Failed to write database to disk: {e}")
            return False
    
    def set(self, key, value):
        """Thêm hoặc cập nhật một key-value vào database."""
        if not self._check_loaded():
            return   # ✅ Trả về danh sách rỗng thay vì lỗi
        key = str(key)
        self.db[key] = value
        self._log("SET", key, value)
        return True
    
    def get(self, key):
        """
        Retrieve the value associated with a key.
        """
        if not self._check_loaded():
            return   # ✅ Trả về danh sách rỗng thay vì lỗi
        key = str(key)
        return self.db.get(key)
    
    def __getitem__(self, key):
        """Cho phép truy cập dữ liệu như dictionary"""
        if not self._check_loaded():
            return   # ✅ Trả về danh sách rỗng thay vì lỗi
        return self.get(key)

    def __setitem__(self, key, value):
        """Cho phép gán giá trị như dictionary"""
        if not self._check_loaded():
            return   # ✅ Trả về danh sách rỗng thay vì lỗi
        self.set(key, value)
    
    def __delitem__(self, key):
        """Cho phép xóa phần tử bằng del db[key]"""
        if not self._check_loaded():
            return   # ✅ Trả về danh sách rỗng thay vì lỗi
        self.remove(key)
    
    def search_by_key(self, substring):
        if not self._check_loaded():
            return   # ✅ Trả về danh sách rỗng thay vì lỗi
        return [key for key in self.db.keys() if substring in key]

    def search_by_value(self, search_value):
        if not self._check_loaded():
            return   # ✅ Trả về danh sách rỗng thay vì lỗi
        return [key for key, value in self.db.items() if search_value in str(value)]

    def filter(self, condition):
        if not self._check_loaded():
            return   # ✅ Trả về danh sách rỗng thay vì lỗi
        return {key: value for key, value in self.db.items() if condition(value)}

    from rapidfuzz import process

    def fuzzy_search(self, search_key, threshold=80):
        if not self._check_loaded():
            return   # ✅ Trả về danh sách rỗng thay vì lỗi
        keys = list(self.db.keys())
        matches = self.process.extract(search_key, keys, limit=5)
        return [match[0] for match in matches if match[1] >= threshold]
    
    def remove(self, key):
        """Xóa một key khỏi database."""
        if not self._check_loaded():
            return   # ✅ Trả về danh sách rỗng thay vì lỗi
        key = str(key)
        if key in self.db:
            del self.db[key]
            self._log("REMOVE", key)
            return True
        return False
    
    def purge(self):
        """Xóa toàn bộ dữ liệu trong database."""
        if not self._check_loaded():
            return   # ✅ Trả về danh sách rỗng thay vì lỗi
        self.db.clear()
        self._log("PURGE", "ALL_KEYS")
        return True
    
    def all(self):
        """
        Retrieve a list of all keys in the database.
        """
        if not self._check_loaded():
            return   # ✅ Trả về danh sách rỗng thay vì lỗi
        return list(self.db.keys())
    
    def set_many(self, data: dict):
        """Thêm nhiều cặp key-value vào database."""
        if not self._check_loaded():
            return   # ✅ Trả về danh sách rỗng thay vì lỗi
        self.db.update(data)
        for key, value in data.items():
            self._log("SET_MANY", key, value)
        return True


    def remove_many(self, keys: list):
        """Xóa nhiều key khỏi database."""
        if not self._check_loaded():
            return   # ✅ Trả về danh sách rỗng thay vì lỗi
        for key in keys:
            if key in self.db:
                self._log("REMOVE_MANY", key)
                self.db.pop(str(key), None)
        return True
    
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