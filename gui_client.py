#!/usr/bin/env python3
"""
gui_client.py

A thread-safe Tkinter GUI frontend for the C++ distributed chat server.
Implements a Queue-based Producer-Consumer pattern to safely pass network
data from the background listening thread to the main Tkinter event loop.
"""

import socket
import threading
import json
import tkinter as tk
from tkinter import scrolledtext, messagebox
import queue

HOST = '127.0.0.1'
PORT = 8080

class ChatClientGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Distributed Chat")
        self.root.geometry("600x700")
        self.root.configure(bg="#1E1E1E")
        
        self.sock = None
        self.username = ""
        
        # Thread-safe queue for cross-thread communication
        self.msg_queue = queue.Queue()
        
        self.build_login_screen()

        # Start the GUI polling loop to check for new messages
        self.root.after(100, self.process_queue)

    def build_login_screen(self):
        """Renders the initial username prompt."""
        self.login_frame = tk.Frame(self.root, bg="#1E1E1E")
        self.login_frame.pack(expand=True, fill=tk.BOTH)
        
        tk.Label(self.login_frame, text="Distributed Chat", font=("Helvetica", 22, "bold"), fg="#FFFFFF", bg="#1E1E1E").pack(pady=(180, 10))
        tk.Label(self.login_frame, text="Choose a username to join the server", font=("Helvetica", 12), fg="#AAAAAA", bg="#1E1E1E").pack(pady=(0, 30))
        
        self.user_entry = tk.Entry(self.login_frame, font=("Helvetica", 16), width=15, bg="#333333", fg="#FFFFFF", insertbackground="white", borderwidth=0, justify="center")
        self.user_entry.pack(pady=10, ipady=8)
        self.user_entry.bind("<Return>", lambda e: self.connect_to_server())
        
        self.connect_btn = tk.Button(self.login_frame, text="Connect", font=("Helvetica", 14, "bold"), bg="#007ACC", fg="white", activebackground="#005A9E", activeforeground="white", command=self.connect_to_server, relief=tk.FLAT)
        self.connect_btn.pack(pady=20, ipadx=30, ipady=8)
        
        self.user_entry.focus()

    def connect_to_server(self):
        """Attempts to establish a TCP connection and transition to the chat UI."""
        self.username = self.user_entry.get().strip()
        if not self.username:
            messagebox.showwarning("Invalid Name", "Please enter a username.")
            return
            
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((HOST, PORT))
            
            # Send initial join broadcast
            join_msg = json.dumps({"type": "system", "user": self.username, "content": f"{self.username} joined the chat!"})
            self.sock.sendall((join_msg + "\n").encode('utf-8'))
            
            # Transition UI
            self.login_frame.destroy()
            self.build_chat_screen()
            
            # Spawn daemon thread to listen for incoming traffic
            threading.Thread(target=self.receive_messages, daemon=True).start()
            
        except Exception as e:
            messagebox.showerror("Connection Error", f"Failed to connect to the server.\nEnsure 'server.exe' is running.\nError: {e}")

    def build_chat_screen(self):
        """Renders the main chat window."""
        self.chat_frame = tk.Frame(self.root, bg="#1E1E1E")
        self.chat_frame.pack(expand=True, fill=tk.BOTH, padx=15, pady=15)
        
        # Read-only scrolling text area
        self.chat_display = scrolledtext.ScrolledText(self.chat_frame, font=("Consolas", 12), bg="#252526", fg="#D4D4D4", state=tk.DISABLED, wrap=tk.WORD, borderwidth=0)
        self.chat_display.pack(expand=True, fill=tk.BOTH, pady=(0, 15))
        
        # Configure text coloring tags
        self.chat_display.tag_config("system", foreground="#E5C07B", font=("Consolas", 11, "italic"))
        self.chat_display.tag_config("user", foreground="#61AFEF", font=("Consolas", 12, "bold"))
        self.chat_display.tag_config("me", foreground="#98C379", font=("Consolas", 12, "bold"))
        self.chat_display.tag_config("msg", foreground="#D4D4D4")

        # Input area
        bottom_frame = tk.Frame(self.chat_frame, bg="#1E1E1E")
        bottom_frame.pack(fill=tk.X)
        
        self.msg_entry = tk.Entry(bottom_frame, font=("Helvetica", 14), bg="#333333", fg="#FFFFFF", insertbackground="white", borderwidth=0)
        self.msg_entry.pack(side=tk.LEFT, expand=True, fill=tk.BOTH, padx=(0, 10), ipady=10)
        self.msg_entry.bind("<Return>", lambda e: self.send_message())
        
        self.send_btn = tk.Button(bottom_frame, text="Send", font=("Helvetica", 12, "bold"), bg="#007ACC", fg="white", activebackground="#005A9E", activeforeground="white", command=self.send_message, relief=tk.FLAT, width=8)
        self.send_btn.pack(side=tk.RIGHT, ipady=6)
        
        self.msg_entry.focus()

    def send_message(self):
        """Constructs and transmits JSON payloads to the server."""
        content = self.msg_entry.get().strip()
        if content and self.sock:
            payload = json.dumps({
                "type": "msg",
                "user": self.username,
                "content": content
            })
            try:
                self.sock.sendall((payload + "\n").encode('utf-8'))
                self.msg_entry.delete(0, tk.END)
                
                # Mirror the message locally since the server doesn't echo back to the sender
                self.display_message({"type": "msg", "user": self.username, "content": content})
            except Exception:
                messagebox.showerror("Network Error", "Lost connection to server.")
                self.root.destroy()

    def receive_messages(self):
        """Background thread logic: Buffers TCP stream, parses JSON, and queues data."""
        buffer = ""
        while True:
            try:
                data = self.sock.recv(1024).decode('utf-8')
                if not data:
                    self.msg_queue.put({"type": "system", "content": "Disconnected from server."})
                    break
                
                buffer += data
                while '\n' in buffer:
                    message_str, buffer = buffer.split('\n', 1)
                    if message_str.strip():
                        try:
                            msg = json.loads(message_str)
                            self.msg_queue.put(msg)
                        except json.JSONDecodeError:
                            pass
            except Exception:
                self.msg_queue.put({"type": "system", "content": "Connection unexpectedly closed."})
                break

    def process_queue(self):
        """Main thread logic: Flushes the queue into the GUI."""
        while not self.msg_queue.empty():
            msg = self.msg_queue.get()
            self.display_message(msg)
            
        # Re-schedule the next queue check
        self.root.after(100, self.process_queue)
        
    def display_message(self, msg):
        """Safely modifies the Tkinter Text widget."""
        self.chat_display.config(state=tk.NORMAL)
        
        msg_type = msg.get("type")
        user = msg.get("user", "Unknown")
        content = msg.get("content", "")
        
        if msg_type == "system":
            self.chat_display.insert(tk.END, f"[System] {content}\n", "system")
        elif msg_type == "msg":
            tag = "me" if user == self.username else "user"
            self.chat_display.insert(tk.END, f"[{user}] ", tag)
            self.chat_display.insert(tk.END, f"{content}\n", "msg")
            
        # Auto-scroll to the bottom
        self.chat_display.yview(tk.END)
        self.chat_display.config(state=tk.DISABLED)

    def on_closing(self):
        """Cleanly closes the socket connection when the window is 'X'd out."""
        if self.sock:
            try:
                leave_msg = json.dumps({"type": "system", "user": self.username, "content": f"{self.username} left the chat."})
                self.sock.sendall((leave_msg + "\n").encode('utf-8'))
                self.sock.close()
            except Exception:
                pass
        self.root.destroy()

if __name__ == "__main__":
    root = tk.Tk()
    app = ChatClientGUI(root)
    # Bind the window close button to our graceful exit function
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()
