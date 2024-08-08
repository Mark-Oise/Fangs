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

    def log(self, start='HEAD'):
        """
        Display the commit history starting from a specified reference.

        This method traverses the commit history, starting from the given reference
        (defaulting to HEAD), and prints out details of each commit encountered.

        Args:
            start (str, optional): The reference to start the log from. Defaults to 'HEAD'.

        Prints:
            For each commit:
            - The commit hash
            - The author
            - The timestamp
            - The commit message

        Note:
            If an error occurs while reading a commit, the method will print an error
            message and stop traversing the history.

        Raises:
            No exceptions are raised directly, but errors are caught and printed.
        """
        try:
            # Start by getting the commit hash that the 'start' reference points to
            current = self.get_ref(start)
            
            while current:
                try:
                    # Attempt to read and parse the commit object
                    commit_data = self.read_object(current, 'commit')
                    
                    # Print commit details
                    print(f'commit {current}')
                    print(f"Author: {commit_data.get('author', 'Unknown')}")
                    print(f"Date: {commit_data.get('timestamp', 'Unknown')}")
                    print(f"\n    {commit_data.get('message', 'No message')}\n")
                    
                    # Move to the parent commit
                    current = commit_data.get('parent')
                    
                    # If there's no parent, we've reached the initial commit
                    if not current:
                        break
                except Exception as e:
                    # If there's an error reading a commit, print it and stop
                    print(f"Error reading commit {current}: {e}")
                    break
        except Exception as e:
            # Handle any errors that occur when starting the log
            print(f"Error starting log from {start}: {e}")
            

    def read_object(self, sha1, expected_type):
        """
        Read an object from the repository.

        Args:
            sha1 (str): The SHA-1 hash of the object.
            expected_type (str): The expected type of the object.

        Returns:
            dict: The object data.

        Raises:
            ValueError: If the object type doesn't match the expected type.
            FileNotFoundError: If the object file doesn't exist.
        """
        path = os.path.join(self.OBJECT_DIR, sha1[:2], sha1[2:])
        try:
            with open(path, 'rb') as f:
                obj = f.read()
            
            # Parse the header
            null_index = obj.index(b'\0')
            header = obj[:null_index].decode()
            obj_type, size = header.split()
           
            
            if obj_type != expected_type:
                raise ValueError(f"Expected {expected_type}, got {obj_type}")
            
            # Parse the data based on the object type
            content = obj[null_index + 1:]
            if obj_type in ('tree', 'commit'):
                return json.loads(content.decode())
            elif obj_type == 'blob':
                return content
            else:
                raise ValueError(f"Unknown object type: {obj_type}")
        except FileNotFoundError:
            raise FileNotFoundError(f"Object {sha1} not found")


    def branch(self, name=None):
        """
        Create a new branch or list existing branches.

        If a name is provided, creates a new branch pointing to the current HEAD commit.
        If no name is provided, lists all existing branches.

        Args:
            name (str, optional): The name of the new branch to create. Defaults to None.

        Raises:
            OSError: If there's an error creating the branch or listing branches.
        """
        if name:
            head_commit = self.get_head_commit()
            if head_commit:
                try:
                    # Create a new branch pointing to the current HEAD commit
                    self.update_ref(f'refs/heads/{name}', head_commit)
                    print(f'Created branch {name}')
                except OSError as e:
                    print(f"Error creating branch: {e}")
            else:
                print('Cannot create branch: no commits yet')
        else:
            # List existing branches
            heads_dir = os.path.join(self.REF_DIR, 'heads')
            try:
                current_branch = self.get_current_branch()
                for branch in os.listdir(heads_dir):
                    # Mark the current branch with an asterisk
                    current = '*' if current_branch == branch else ' '
                    print(f'{current} {branch}')
            except OSError as e:
                print(f"Error listing branches: {e}")

    def get_current_branch(self):
        """
        Get the name of the current branch.

        Returns:
            str or None: The name of the current branch, or None if not on a branch.

        Raises:
            IOError: If there's an error reading the HEAD file.
        """
        try:
            with open(self.HEAD_FILE, 'r') as f:
                content = f.read().strip()
                # Check if HEAD is pointing to a branch reference
                if content.startswith('ref: refs/heads/'):
                    return content[16:]  # Return branch name
        except IOError as e:
            print(f"Error reading HEAD file: {e}")
        return None  # Return None if not on a branch or if there's an error


     def checkout(self, branch_name):
        """
        Switch to a different branch and update the working directory.

        Args:
            branch_name (str): The name of the branch to checkout.

        Raises:
            OSError: If there's an error reading or writing files.
        """
        branch_ref = f'refs/heads/{branch_name}'
        if not os.path.exists(os.path.join(self.FANGS_DIR, branch_ref)):
            print(f"Branch '{branch_name}' does not exist")
            return
        
        try:
            # Update HEAD
            with open(self.HEAD_FILE, 'w') as f:
                f.write(f'ref: {branch_ref}')
        
            # Get the commit that the branch points to
            branch_commit = self.get_ref(branch_ref)
        
            # Read the tree from the commit
            commit_data = self.read_object(branch_commit, 'commit')
            tree_sha1 = commit_data['tree']
            tree = self.read_object(tree_sha1, 'tree')
        
            # Update working directory
            self.update_working_directory(tree)
        
            print(f"Switched to branch '{branch_name}'")
        except OSError as e:
            print(f"Error during checkout: {e}")
            # TODO: Consider adding rollback logic here

    def update_working_directory(self, tree):
        """
        Update the working directory to match the given tree.

        Args:
            tree (dict): A dictionary representing the file structure.

        Raises:
            OSError: If there's an error manipulating files or directories.
        """
        try:
            # Remove all files in the working directory except .fangs
            for root, dirs, files in os.walk(self.repo_path, topdown=False):
                if '.fangs' in dirs:
                    dirs.remove('.fangs')
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))

            # Write files from the tree to the working directory
            for path, sha1 in tree.items():
                file_path = os.path.join(self.repo_path, path)
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                with open(file_path, 'wb') as f:
                    f.write(self.read_object(sha1, 'blob'))
        except OSError as e:
            print(f"Error updating working directory: {e}")
            # Consider adding rollback logic here
        
    def merge(self, branch_name):
        """
        Merge the specified branch into the current branch.

        Args:
            branch_name (str): The name of the branch to merge.

        Raises:
            ValueError: If not on a branch or if the specified branch doesn't exist.
        """
        # Check if we're on a branch
        current_branch = self.get_current_branch()
        if not current_branch:
            raise ValueError('Not currently on any branch')
        
        # Get the current and target commit SHAs
        current_commit = self.get_head_commit()
        other_commit = self.get_ref(f'refs/heads/{branch_name}')
        
        if not other_commit:
            raise ValueError(f'Branch {branch_name} does not exist')

        # Find the common ancestor of the two branches
        base_commit = self.find_merge_base(current_commit, other_commit)

        # Determine the type of merge required
        if base_commit == other_commit:
            print('Already up-to-date')
            return
        elif base_commit == current_commit:
            self.fast_forward_merge(other_commit, branch_name)
        else:
            self.three_way_merge(current_commit, other_commit, base_commit, branch_name)

    def find_merge_base(self, commit1, commit2):
        """
        Find the most recent common ancestor of two commits.

        Args:
            commit1 (str): SHA-1 of the first commit.
            commit2 (str): SHA-1 of the second commit.

        Returns:
            str: SHA-1 of the merge base commit, or None if not found.
        """
        # Get the commit chains for both commits
        chain1 = self.get_commit_chain(commit1)
        chain2 = self.get_commit_chain(commit2)

        # Iterate through the first chain to find the first common commit
        for commit in chain1:
            if commit in chain2:
                return commit  # Found the merge base

        # If no common commit is found, return None
        return None

    def get_commit_chain(self, commit):
        """
        Get the chain of commits starting from the given commit.

        Args:
            commit (str): SHA-1 of the starting commit.

        Returns:
            list: List of commit SHA-1s in the chain, oldest first.
        """
        chain = []
        current = commit

        # Traverse the commit history
        while current:
            chain.append(current)
            commit_data = self.read_object(current, 'commit')
            current = commit_data.get('parent')  # Move to the parent commit

        # Reverse the chain to get oldest commit first
        return list(reversed(chain))

    def fast_forward_merge(self, target_commit, branch_name):
        """
        Perform a fast-forward merge to the target commit.

        Args:
            target_commit (str): The SHA-1 of the commit to merge to.
            branch_name (str): The name of the branch being merged.
        """
        # Update HEAD to point to the target commit
        self.update_ref('HEAD', target_commit)
        print(f'Fast-forwarded merge to {branch_name}')
        # Update the working directory to match the target commit
        self.update_working_directory(self.get_tree(target_commit))

    def three_way_merge(self, current_commit, other_commit, base_commit, branch_name):
        """
        Perform a three-way merge between the current branch, the other branch, and their common ancestor.

        Args:
            current_commit (str): The SHA-1 of the current branch's head commit.
            other_commit (str): The SHA-1 of the other branch's head commit.
            base_commit (str): The SHA-1 of the common ancestor commit.
            branch_name (str): The name of the branch being merged.
        """
        # Get the tree objects for all three commits
        base_tree = self.get_tree(base_commit)
        current_tree = self.get_tree(current_commit)
        other_tree = self.get_tree(other_commit)

        merged_tree = {}
        conflicts = []

        # Get all unique files across all three trees
        all_files = set(base_tree.keys()) | set(current_tree.keys()) | set(other_tree.keys())

        for file in all_files:
            base_sha = base_tree.get(file)
            current_sha = current_tree.get(file)
            other_sha = other_tree.get(file)

            # Determine how to handle each file
            if current_sha == other_sha:
                # File is the same in both branches or doesn't exist in one
                if current_sha:
                    merged_tree[file] = current_sha
            elif current_sha == base_sha:
                # File was not changed in current branch, use other branch version
                merged_tree[file] = other_sha
            elif other_sha == base_sha:
                # File was not changed in other branch, use current branch version
                merged_tree[file] = current_sha
            else:
                # Conflict: file changed in both branches
                conflicts.append(file)
                merged_tree[file] = self.create_conflict_file(file, base_sha, current_sha, other_sha, branch_name)

        if conflicts:
            self.handle_merge_conflicts(conflicts, merged_tree)
        else:
            self.create_merge_commit(current_commit, other_commit, merged_tree, branch_name)

    def handle_merge_conflicts(self, conflicts, merged_tree):
        """
        Handle merge conflicts by notifying the user and updating the working directory.

        Args:
            conflicts (list): List of files with conflicts.
            merged_tree (dict): The merged tree structure.
        """
        print('Merge conflicts in files:')
        for file in conflicts:
            print(f' {file}')
        print('Resolve conflicts and commit the results')
        # Update working directory with conflict markers
        self.update_working_directory(merged_tree)

    def create_merge_commit(self, current_commit, other_commit, merged_tree, branch_name):
        """
        Create a merge commit after a successful merge.

        Args:
            current_commit (str): The SHA-1 of the current branch's head commit.
            other_commit (str): The SHA-1 of the other branch's head commit.
            merged_tree (dict): The merged tree structure.
            branch_name (str): The name of the branch being merged.
        """
        merge_commit_message = f"Merge branch '{branch_name}'"
        # Hash the merged tree structure
        merged_tree_sha = self.hash_object(json.dumps(merged_tree).encode(), 'tree')
        # Create the merge commit data
        merge_commit_data = {
            'tree': merged_tree_sha,
            'parents': [current_commit, other_commit],
            'author': 'Simple Fangs User <user@example.com>',
            'timestamp': datetime.now().isoformat(),
            'message': merge_commit_message
        }
        # Create the merge commit object
        merge_commit_sha = self.hash_object(json.dumps(merge_commit_data).encode(), 'commit')
        # Update HEAD to point to the new merge commit
        self.update_ref('HEAD', merge_commit_sha)
        print(f"Merged branch '{branch_name}' into {self.get_current_branch()}")
        # Update the working directory to reflect the merged state
        self.update_working_directory(merged_tree)

    def get_tree(self, commit_sha):
        """
        Get the tree object associated with a commit.

        Args:
            commit_sha (str): The SHA-1 of the commit.

        Returns:
            dict: The tree structure of the commit.
        """
        # Read the commit object to get the tree SHA
        commit_data = self.read_object(commit_sha, 'commit')
        # Read and return the tree object
        return self.read_object(commit_data['tree'], 'tree')

    def create_conflict_file(self, file, base_sha, current_sha, other_sha, branch_name):
        """
        Create a conflict file with markers for manual resolution.

        Args:
            file (str): The name of the conflicting file.
            base_sha (str): The SHA-1 of the base version.
            current_sha (str): The SHA-1 of the current branch version.
            other_sha (str): The SHA-1 of the other branch version.
            branch_name (str): The name of the branch being merged.

        Returns:
            str: The SHA-1 of the created conflict file.
        """
        # Generate content with conflict markers
        content = f"""<<<<<<< HEAD
{self.read_object(current_sha, 'blob').decode() if current_sha else ''}
=======
{self.read_object(other_sha, 'blob').decode() if other_sha else ''}
>>>>>>> {branch_name}
"""
        # Hash and store the conflict file content
        return self.hash_object(content.encode(), 'blob')

    def status(self):
        """
        Display the current status of the repository.

        This method shows:
        1. The current branch
        2. Staged changes (files ready to be committed)
        3. Unstaged changes (modified files not yet staged)
        4. Untracked files (files not under version control)
        """
        # Get and display the current branch
        current_branch = self.get_current_branch()
        print(f'On branch {current_branch}')

        # Get and display staged changes
        staged_changes = self.get_staged_changes()
        if staged_changes:
            print("\nChanges to be committed:")
            for file, status in staged_changes.items():
                print(f"  {status}: {file}")

        # Get and display unstaged changes
        unstaged_changes = self.get_unstaged_changes()
        if unstaged_changes:
            print("\nChanges not staged for commit:")
            for file, status in unstaged_changes.items():
                print(f"  {status}: {file}")

        # Get and display untracked files
        untracked_files = self.get_untracked_files()
        if untracked_files:
            print("\nUntracked files:")
            for file in untracked_files:
                print(f"  {file}")

    def get_staged_changes(self):
        """
        Get a dictionary of staged changes.

        This method compares the current index with the HEAD tree to determine
        which files have been staged for commit.

        Returns:
            dict: A dictionary where keys are file paths and values are their status
                  ('new file', 'modified', or 'deleted').
        """
        staged_changes = {}
        index_file = os.path.join(self.FANGS_DIR, 'index')

        # If index doesn't exist, there are no staged changes
        if not os.path.exists(index_file):
            return staged_changes

        # Get the tree of the current HEAD commit
        head_tree = self.get_head_tree()

        # Compare index with HEAD tree
        with open(index_file, 'r') as f:
            for line in f:
                sha1, path = line.strip().split(' ', 1)
                # If file is new or modified compared to HEAD, it's staged
                if path not in head_tree or head_tree[path] != sha1:
                    staged_changes[path] = 'new file' if path not in head_tree else 'modified'

        # Check for deleted files (in HEAD but not in working directory)
        for path in head_tree:
            if path not in staged_changes and not os.path.exists(os.path.join(self.repo_path, path)):
                staged_changes[path] = 'deleted'

        return staged_changes

    def get_unstaged_changes(self):
        """
        Get a dictionary of unstaged changes.

        This method compares the working directory with the current index
        to determine which files have been modified but not staged.

        Returns:
            dict: A dictionary where keys are file paths and values are their status
                  ('modified', 'new file', or 'deleted').
        """
        unstaged_changes = {}
        index_file = os.path.join(self.FANGS_DIR, 'index')

        # If index doesn't exist, consider all files as unstaged
        if not os.path.exists(index_file):
            return self.get_untracked_files()

        # Read the current index
        with open(index_file, 'r') as f:
            index = dict(line.strip().split(' ', 1) for line in f)

        # Compare working directory with index
        for root, _, files in os.walk(self.repo_path):
            if '.fangs' in root:
                continue  # Skip the .fangs directory
            for file in files:
                path = os.path.relpath(os.path.join(root, file), self.repo_path)
                with open(os.path.join(root, file), 'rb') as f:
                    content = f.read()
                sha1 = self.hash_object(content, 'blob')
                if path in index:
                    if index[path] != sha1:
                        unstaged_changes[path] = 'modified'
                else:
                    unstaged_changes[path] = 'new file'

        # Check for deleted files (in index but not in working directory)
        for path in index:
            if not os.path.exists(os.path.join(self.repo_path, path)):
                unstaged_changes[path] = 'deleted'

        return unstaged_changes

    def get_head_tree(self):
        """
        Get the tree object of the current HEAD commit.

        This method retrieves the tree structure associated with the current
        HEAD commit, which represents the last known good state of the repository.

        Returns:
            dict: A dictionary representing the tree structure of the HEAD commit,
                  or an empty dict if there's no HEAD commit.
        """
        # Get the SHA-1 of the current HEAD commit
        head_commit = self.get_head_commit()
        if not head_commit:
            return {}  # Return empty dict if there's no HEAD commit

        # Read the commit object to get the tree SHA-1
        commit_data = self.read_object(head_commit, 'commit')

        # Read and return the tree object
        return self.read_object(commit_data['tree'], 'tree')

    def get_untracked_files(self):
        """
        Get a list of untracked files in the repository.

        This method identifies files in the working directory that are not
        tracked by FANGS (i.e., not in the index).

        Returns:
            list: A list of file paths that are not tracked.
        """
        untracked_files = []
        index_file = os.path.join(self.FANGS_DIR, 'index')

        # If index doesn't exist, all files are untracked
        if not os.path.exists(index_file):
            for root, _, files in os.walk(self.repo_path):
                if '.fangs' in root:
                    continue  # Skip the .fangs directory
                for file in files:
                    untracked_files.append(os.path.relpath(os.path.join(root, file), self.repo_path))
            return untracked_files

        # Read the current index
        with open(index_file, 'r') as f:
            index = dict(line.strip().split(' ', 1) for line in f)

        # Check all files in the working directory
        for root, _, files in os.walk(self.repo_path):
            if '.fangs' in root:
                continue  # Skip the .fangs directory
            for file in files:
                path = os.path.relpath(os.path.join(root, file), self.repo_path)
                if path not in index:
                    untracked_files.append(path)

        return untracked_files



   