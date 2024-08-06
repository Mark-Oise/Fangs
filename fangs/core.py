import os
import hashlib
from datetime import datetime
import json
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

    def commit(self, message):
        """
        Create a new commit with the current index state.

        Args:
            message (str): The commit message.

        Raises:
            ValueError: If the message is empty or not a string.
            OSError: If there's an error reading the index or writing the commit.

        Returns:
            str: The SHA-1 hash of the new commit.
        """
        # Validate input
        if not isinstance(message, str) or not message.strip():
            raise ValueError("Commit message must be a non-empty string")

        # Check if there's anything to commit
        index_file = os.path.join(self.FANGS_DIR, 'index')
        if not os.path.exists(index_file):
            print("Nothing to commit")
            return None

        try:
            # Read the index and create a tree structure
            tree = {}
            with open(index_file, 'r') as f:
                for line in f:
                    sha1, path = line.strip().split(' ', 1)
                    tree[path] = sha1

            # Hash the tree structure
            tree_sha1 = self.hash_object(json.dumps(tree).encode(), 'tree')

            # Prepare commit data
            commit_data = {
                'tree': tree_sha1,
                'parent': self.get_head_commit(),  # Get the current HEAD commit
                'author': 'Simple Fangs User <user@example.com>',
                'timestamp': datetime.now().isoformat(),
                'message': message
            }

            # Hash the commit data to create a new commit object
            commit_sha1 = self.hash_object(json.dumps(commit_data).encode(), 'commit')

            # Update the HEAD reference to point to the new commit
            self.update_ref('HEAD', commit_sha1)

            print(f"Committed changes: {commit_sha1}")
            return commit_sha1

        except OSError as e:
            # Handle file-related errors
            raise OSError(f"Error creating commit: {e}")
        except Exception as e:
            # Catch any other unexpected errors
            raise RuntimeError(f"Unexpected error during commit: {e}")

    def get_head_commit(self):
        """
        Get the current HEAD commit.

        Returns:
            str or None: The SHA-1 hash of the current HEAD commit, or None if not found.
        """
        return self.get_ref('HEAD')

    def get_ref(self, ref):
        """
        Get the SHA-1 hash that a reference points to.

        This method resolves references, including symbolic references.

        Args:
            ref (str): The name of the reference to resolve.

        Returns:
            str or None: The SHA-1 hash the reference points to, or None if not found.

        Raises:
            IOError: If there's an error reading the reference file.
        """
        ref_file = os.path.join(self.FANGS_DIR, ref)
        try:
            if os.path.exists(ref_file):
                with open(ref_file, 'r') as f:
                    content = f.read().strip()
                    # Check if it's a symbolic reference
                    if content.startswith('ref: '):
                        # Recursively resolve the symbolic reference
                        return self.get_ref(content[5:])
                    # Return the SHA-1 hash
                    return content
            else:
                # Reference file doesn't exist
                print(f"Warning: Reference '{ref}' not found.")
                return None
        except IOError as e:
            # Handle potential file reading errors
            print(f"Error reading reference '{ref}': {e}")
            return None

    def update_ref(self, ref, sha1):
        """
        Update a reference to point to a specific commit.

        Args:
            ref: The name of the reference to update (e.g., 'HEAD', 'refs/heads/master').
            sha1: The SHA-1 hash of the commit to point to.

        Raises:
            ValueError: If ref or sha1 is empty or not a string.
            OSError: If there's an error creating directories or writing to the file.
        """
        # Input validation
        if not isinstance(ref, str) or not ref.strip():
            raise ValueError("ref must be a non-empty string")
        if not isinstance(sha1, str) or not sha1.strip():
            raise ValueError("sha1 must be a non-empty string")

        # Construct the full path to the reference file
        ref_file = os.path.join(self.FANGS_DIR, ref)

        try:
            # Ensure the directory structure exists
            os.makedirs(os.path.dirname(ref_file), exist_ok=True)

            # Write the SHA-1 hash to the reference file
            with open(ref_file, 'w') as f:
                f.write(sha1)
        except OSError as e:
            # Handle potential file system errors
            raise OSError(f"Error updating reference '{ref}': {e}")

        # Optionally, log the update
        print(f"Updated reference '{ref}' to point to {sha1}")