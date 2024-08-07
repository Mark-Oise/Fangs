import argparse
from .core import FANGS

def main():
    """
    Main entry point for the FANGS CLI application.
    Parses command-line arguments and executes the corresponding FANGS commands.
    """
    parser = argparse.ArgumentParser(description="FANGS - Files And New Git System")
    parser.add_argument('command', help="Fangs command to execute")
    parser.add_argument('args', nargs='*', help="Command arguments")

    args = parser.parse_args()

    repo = FANGS('.')

    # Dictionary mapping commands to their corresponding methods
    commands = {
        'init': lambda: repo.init(),
        'add': lambda: [repo.add(file) for file in args.args],
        'commit': lambda: repo.commit(' '.join(args.args)),
        'log': lambda: repo.log(),
        'branch': lambda: repo.branch(args.args[0] if args.args else None),
        'checkout': lambda: repo.checkout(args.args[0]),
        'merge': lambda: repo.merge(args.args[0]),
        'status': lambda: repo.status()
    }

    # Execute the command if it exists, otherwise print an error message
    if args.command in commands:
        commands[args.command]()
    else:
        print(f'Unknown command: {args.command}')

if __name__ == "__main__":
    main()