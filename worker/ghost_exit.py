from pynput import mouse, keyboard
import time
import threading

class ActivityMonitor:
    def __init__(self, idle_threshold_sec=5):
        """
        idle_threshold_sec: How many seconds of no mouse/keyboard 
        input must pass before the computer is considered "Idle".
        """
        self.last_activity_time = time.time()
        self.idle_threshold_sec = idle_threshold_sec

        # Set up OS-level listeners
        self.mouse_listener = mouse.Listener(
            on_move=self.on_activity, 
            on_click=self.on_activity, 
            on_scroll=self.on_activity
        )
        self.keyboard_listener = keyboard.Listener(
            on_press=self.on_activity
        )

    def on_activity(self, *args, **kwargs):
        """Fires every time the mouse moves or a key is pressed."""
        self.last_activity_time = time.time()

    def start(self):
        """Starts the background listening threads."""
        self.mouse_listener.start()
        self.keyboard_listener.start()
        print("[+] Ghost Exit OS Hook active: Monitoring hardware interrupts.")

    def stop(self):
        self.mouse_listener.stop()
        self.keyboard_listener.stop()

    def is_user_active(self) -> bool:
        """Returns True if the user has interacted recently."""
        return (time.time() - self.last_activity_time) < self.idle_threshold_sec