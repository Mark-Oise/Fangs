import os
import hashlib
from typing import Optional


class FANGS:
    """
    A class representing the FANGS version control system.

    This class provides the core functionality for initializing and managing
    a FANGS repository.
    """

    def __init__(self, repo_path):
        """
        Initialize the FANGS class.

        Args:
            repo_path: The path to the repository.

        Raises:
            ValueError: If the repo_path is empty or not a string.
        """
        if not isinstance(repo_path, str) or not repo_path.strip():
            raise ValueError("repo_path must be a non-empty string")

        self.repo_path = repo_path
        self.FANGS_DIR = os.path.join(repo_path, 'fangs')
        self.OBJECT_DIR = os.path.join(self.FANGS_DIR, 'objects')
        self.REF_DIR = os.path.join(self.FANGS_DIR, 'refs')
        self.HEAD_FILE = os.path.join(self.FANGS_DIR, 'HEAD')

    def init(self):
        """
        Initialize a new FANGS repository.

        This method creates the necessary directory structure for a FANGS repository
        and initializes the HEAD file to point to the master branch.

        If the repository already exists, it prints a message indicating so.

        Raises:
            OSError: If there's an error creating directories or writing to the HEAD file.
        """
        if not os.path.exists(self.FANGS_DIR):
            try:
                # Create the main FANGS directory
                os.makedirs(self.FANGS_DIR)
                # Create the objects directory to store all FANGS objects
                os.makedirs(self.OBJECT_DIR)
                # Create the refs/heads directory for branch references
                os.makedirs(os.path.join(self.REF_DIR, 'heads'))
                # Initialize the HEAD file to point to the master branch
                with open(self.HEAD_FILE, 'w') as f:
                    f.write('ref: refs/heads/master')
                print(f'Initialized empty FANGS repository in {self.FANGS_DIR}')
            except OSError as e:
                print(f"Error initializing repository: {e}")
                raise
        else:
            print(f'Repository already exists in {self.FANGS_DIR}')

    def hash_object(self, data, obj_type):
        """
        Hash and store an object in the FANGS repository.

        Args:
            data: The content of the object to hash and store (must be bytes).
            obj_type: The type of the object (e.g., 'blob', 'tree', 'commit').

        Returns:
            The SHA-1 hash of the object.

        Raises:
            ValueError: If data is not bytes or obj_type is not a non-empty string.
            OSError: If there's an error creating directories or writing the file.
        """
        # Input validation
        if not isinstance(data, bytes):
            raise ValueError("data must be bytes")
        if not isinstance(obj_type, str) or not obj_type.strip():
            raise ValueError("obj_type must be a non-empty string")

        # Prepare the object data with header
        header = f'{obj_type}{len(data)}\0'
        full_data = header.encode() + data

        # Generate SHA-1 hash
        sha1 = hashlib.sha1(full_data).hexdigest()

        # Determine the path where the object will be stored
        # Objects are stored in subdirectories based on the first two characters of their hash
        path = os.path.join(self.OBJECT_DIR, sha1[:2], sha1[2:])

        try:
            # Only write the object if it doesn't already exist
            if not os.path.exists(path):
                # Create the directory if it doesn't exist
                os.makedirs(os.path.dirname(path), exist_ok=True)
                # Write the object data to the file
                with open(path, 'wb') as f:
                    f.write(full_data)
        except OSError as e:
            # Raise a more informative error if file operations fail
            raise OSError(f"Error writing object to {path}: {e}")

        # Return the SHA-1 hash of the object
        return sha1

    def add(self, file_path):
        """
        Add a file to the FANGS index.

        This method reads the content of the specified file, hashes it,
        and adds an entry to the index file.

        Args:
            file_path: The path to the file to be added.

        Raises:
            ValueError: If the file_path is empty or not a string.
            FileNotFoundError: If the specified file does not exist.
            OSError: If there's an error reading the file or writing to the index.

        Returns:
            None
        """
        # Validate input
        if not isinstance(file_path, str) or not file_path.strip():
            raise ValueError("file_path must be a non-empty string")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        if not os.path.isfile(file_path):
            raise ValueError(f"Not a file: {file_path}")

        # Ensure the file is within the repository
        try:
            relative_path = os.path.relpath(file_path, self.repo_path)
            if relative_path.startswith('..'):
                raise ValueError(f"File is outside the repository: {file_path}")
        except ValueError as e:
            raise ValueError(f"Invalid file path: {e}")

        try:
            # Read the file content
            with open(file_path, 'rb') as f:
                data = f.read()

            # Hash the file content
            sha1 = self.hash_object(data, 'blob')

            # Create the index entry
            index_entry = f'{sha1} {relative_path}'
            index_file = os.path.join(self.FANGS_DIR, 'index')

            # Update the index file, replacing existing entries if necessary
            updated = False
            temp_index_file = index_file + '.temp'
            with open(index_file, 'r') as old_f, open(temp_index_file, 'w') as new_f:
                for line in old_f:
                    # If the file is already in the index, update its entry
                    if line.split()[1] == relative_path:
                        new_f.write(index_entry + '\n')
                        updated = True
                    else:
                        new_f.write(line)
                # If it's a new file, add it to the end of the index
                if not updated:
                    new_f.write(index_entry + '\n')

            # Replace the old index file with the updated one
            os.replace(temp_index_file, index_file)
            print(f'Added {relative_path} to index')

        except OSError as e:
            raise OSError(f"Error adding file to index: {e}")

    
        index_file = os.path.join(self.FANGS_DIR, 'index')
        if not os.path.exist(index_file):
            print(f'Nothing to commit')
            return
        
        tree = {}
        with open(index, 'r') as f:
            for line in f:
                sha1, path = line.strip().split('', 1)

