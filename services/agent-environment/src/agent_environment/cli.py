import threading
import uvicorn
import sys
import time
import tty
import termios
import urllib.request
from agent_environment.main import app

# ANSI Colors
GREEN = "\033[92m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"

def start_interactive_cli():
    # Configure uvicorn
    config = uvicorn.Config(app, host="0.0.0.0", port=1984, log_level="info")
    server = uvicorn.Server(config)

    # Function to handle user input
    def input_loop():
        # Give the server a moment to start logging
        time.sleep(5)  # Wait for server startup logs to finish
        
        fd = sys.stdin.fileno()
        # Save original terminal settings
        try:
            old_settings = termios.tcgetattr(fd)
        except termios.error:
            # Handle case where stdin is not a TTY (e.g. during build or non-interactive run)
            print(f"{RED}Warning: Not a TTY. Interactive mode disabled.{RESET}")
            return

        try:
            # Set terminal to cbreak mode (single char input, but allows Ctrl+C etc)
            tty.setcbreak(fd)
            while True:
                char = sys.stdin.read(1)
                if char.lower() == 'q':
                    print(f"\n{YELLOW}[CLI] Releasing sandboxes...{RESET}")
                    try:
                        req = urllib.request.Request("http://127.0.0.1:1984/admin/release-sandboxes", method="POST")
                        with urllib.request.urlopen(req) as response:
                             print(f"{GREEN}[CLI] Sandboxes released successfully! (You can now press CTRL+C to quit){RESET}")
                    except Exception as e:
                        print(f"{RED}[CLI] Failed to release sandboxes: {e}{RESET}")
        except Exception as e:
            pass
        finally:
            # Ensure settings are restored
            try:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            except:
                pass
    
    # Start input loop in a separate thread
    input_thread = threading.Thread(target=input_loop, daemon=True)
    input_thread.start()

    # Run server (this blocks until shutdown)
    try:
        server.run()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    try:
        start_interactive_cli()
    except KeyboardInterrupt:
        pass
