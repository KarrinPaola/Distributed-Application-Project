"""
Copyright Harrison Erd

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its
contributors may be used to endorse or promote products derived from this
software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import datetime
import os
import orjson

class PickleDB:
    """
    A barebones orjson-based key-value store with essential methods: set,
    get, save, remove, purge, and all.
    """

# Thêm tính năng sao lưu dữ liệu
    def __init__(self, location, backup_dir="backups"):
        """
        Initialize the pkldb object.

        Args:
            location (str): Path to the JSON file.
        """
        # Tạo đường dẫn đầy đủ cho back up
        self.backup_dir = os.path.expanduser(backup_dir)
        # Tạo thư mục backup nếu chưa có
        os.makedirs(self.backup_dir, exist_ok=True)  
        
        # Tạo đường dẫn đầy đủ cho csdl chính
        self.location = os.path.expanduser(location)
        
        self._load()
    
    def _create_backup(self):
        """Tạo một bản sao lưu hiện tại của cơ sở dữ liệu."""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(self.backup_dir, f"backup_{timestamp}.json")
        try:
            with open(backup_path, 'wb') as backup_file:
                backup_file.write(orjson.dumps(self.db))
            print(f"Backup created: {backup_path}")
        except Exception as e:
            print(f"Failed to create backup: {e}")

    def _load(self):
        """
        Load data from the JSON file if it exists, or initialize an empty
        database.
        """
        if (os.path.exists(self.location) and
                os.path.getsize(self.location) > 0):
            try:
                with open(self.location, 'rb') as f:
                    self.db = orjson.loads(f.read())
                    print("Database loaded")
            except Exception as e:
                self.db = {}
                print(f"Failed to load database: {e}")
        else:
            self.db = {}
            print("Database created")

    def __setitem__(self, key, value):
        return self.set(key, value)

    def __getitem__(self, key):
        return self.get(key)

    def save(self):
        """
        Save the database to the file using an atomic save.

        Behavior:
            - Writes to a temporary file and replaces the
              original file only after the write is successful,
              ensuring data integrity.

        Returns:
            bool: True if save was successful, False if not.
        """
        temp_location = f"{self.location}.tmp"
        try:
            with open(temp_location, 'wb') as temp_file:
                temp_file.write(orjson.dumps(self.db))
            os.replace(temp_location, self.location)  # Atomic replace
            # Tạo bản sao lưu
            self._create_backup()  
            return True
        except Exception as e:
            print(f"Failed to write database to disk: {e}")
            return False

    def set(self, key, value):
        """
        Add or update a key-value pair in the database.

        Args:
            key (any): The key to set. If the key is not a string, it will be
                       converted to a string.
            value (any): The value to associate with the key.

        Returns:
            bool: True if the operation succeeds.

        Behavior:
            - If the key already exists, its value will be updated.
            - If the key does not exist, it will be added to the database.
        """
        key = str(key) if not isinstance(key, str) else key
        self.db[key] = value
        return True

    def remove(self, key):
        """
        Remove a key and its value from the database.

        Args:
            key (any): The key to delete. If the key is not a string, it will
             be converted to a string.

        Returns:
            bool: True if the key was deleted, False if the key does not exist.
        """
        key = str(key) if not isinstance(key, str) else key
        if key in self.db:
            del self.db[key]
            return True
        return False

    def purge(self):
        """
        Clear all keys from the database.

        Returns:
            bool: True if the operation succeeds.
        """
        self.db.clear()
        return True

    def get(self, key):
        """
        Get the value associated with a key.

        Args:
            key (any): The key to retrieve. If the key is not a string, it will
                       be converted to a string.

        Returns:
            any: The value associated with the key, or None if the key does
            not exist.
        """
        key = str(key) if not isinstance(key, str) else key
        return self.db.get(key)

    def all(self):
        """
        Get a list of all keys in the database.

        Returns:
            list: A list of all keys.
        """
        return list(self.db.keys())

    def list_backups(self):
        """Liệt kê tất cả các bản sao lưu."""
        backups = sorted(os.listdir(self.backup_dir))
        return backups
    
    def restore(self, backup_filename):
        """Khôi phục cơ sở dữ liệu từ một file backup."""
        backup_path = os.path.join(self.backup_dir, backup_filename)
        if not os.path.exists(backup_path):
            print(f"Backup file not found: {backup_filename}")
            return False
        try:
            with open(backup_path, 'rb') as backup_file:
                self.db = orjson.loads(backup_file.read())
            self.save()  # Ghi lại dữ liệu vừa khôi phục vào file chính
            print(f"Database restored from {backup_filename}")
            return True
        except Exception as e:
            print(f"Failed to restore backup: {e}")
            return False