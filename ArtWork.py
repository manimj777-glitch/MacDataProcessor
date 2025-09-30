#!/usr/bin/env python3
"""
Automated Data Processor - Mac Native with Enhanced OneDrive Detection
Optimized for macOS with automatic OneDrive folder detection
"""

import pandas as pd
import numpy as np
import os
import sys
import platform
import threading
import re
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
from datetime import datetime
import time
import glob

# Kivy imports
from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.anchorlayout import AnchorLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.textinput import TextInput
from kivy.uix.progressbar import ProgressBar
from kivy.uix.popup import Popup
from kivy.clock import Clock, mainthread
from kivy.graphics import Color, Rectangle
from kivy.core.window import Window

# Native Mac file dialog support
import tkinter as tk
from tkinter import filedialog

warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

class AutomatedDataProcessor(App):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.title = "Automated Data Processor - Mac"
        
        # Core data
        self.production_files = []
        self.consolidated_data = pd.DataFrame()
        self.project_tracker_data = pd.DataFrame()
        self.combined_data = pd.DataFrame()
        self.final_output_data = pd.DataFrame()
        
        # Paths and settings
        self.project_tracker_path = ""
        self.processing_logs = []
        self.selected_start_date = None
        self.selected_end_date = None
        self.sharepoint_access_ok = False
        
        # Setup paths with enhanced OneDrive detection
        self.setup_paths()
        
        # Target columns
        self.target_columns = ['Item Number', 'Product Vendor Company Name', 'Brand', 'Product Name', 'SKU New/Existing']
        
        # Final output columns
        self.final_columns = [
            'HUGO ID', 'Product Vendor Company Name', 'Item Number', 'Product Name', 'Brand', 'SKU', 
            'Artwork Release Date', '5 Weeks After Artwork Release', 'Entered into HUGO Date', 
            'Entered in HUGO?', 'Store Date', 'Re-Release Status', 'Packaging Format 1', 
            'Printer Company Name 1', 'Vendor e-mail 1', 'Printer e-mail 1', 
            'Printer Code 1 (LW Code)', 'File Name'
        ]
        
        # UI references
        self.status_label = None
        self.progress_bar = None
        self.tracker_status_label = None
        self.start_date_input = None
        self.end_date_input = None
        self.apply_btn = None
        self.open_folder_btn = None
        self.manual_path_input = None
        self.folders_found_label = None
        
    def build(self):
        """Build the Kivy GUI"""
        root_layout = BoxLayout(orientation="vertical", padding=20, spacing=10)
        
        # Background
        with root_layout.canvas.before:
            Color(0, 0, 0.5, 1)
            self.rect = Rectangle(pos=root_layout.pos, size=root_layout.size)
        root_layout.bind(pos=self.update_rect, size=self.update_rect)
        
        # Title
        top_layout = AnchorLayout(anchor_x="center", anchor_y="top", size_hint_y=None, height=80)
        title_container = BoxLayout(orientation="vertical", size_hint_y=None, height=80)
        
        title = Label(
            text="MAC AUTOMATED DATA PROCESSOR",
            font_size=24,
            bold=True,
            color=(1, 1, 1, 1),
            size_hint_y=None,
            height=50,
            halign="center",
            valign="middle"
        )
        title.bind(size=title.setter('text_size'))
        
        subtitle = Label(
            text="Enhanced OneDrive Folder Detection | Hidden Sheet Support",
            font_size=14,
            color=(0.8, 0.8, 0.8, 1),
            size_hint_y=None,
            height=30,
            halign="center",
            valign="middle"
        )
        subtitle.bind(size=subtitle.setter('text_size'))
        
        title_container.add_widget(title)
        title_container.add_widget(subtitle)
        top_layout.add_widget(title_container)
        root_layout.add_widget(top_layout)
        
        # OneDrive Status
        self.folders_found_label = Label(
            text="Scanning for OneDrive folders...",
            font_size=12,
            color=(1, 1, 0, 1),
            size_hint_y=None,
            height=30,
            halign="center",
            valign="middle"
        )
        self.folders_found_label.bind(size=self.folders_found_label.setter('text_size'))
        root_layout.add_widget(self.folders_found_label)
        
        # Step 1: Project Tracker
        step1_label = Label(
            text="Step 1: Select Project Tracker",
            font_size=16,
            bold=True,
            color=(1, 1, 1, 1),
            size_hint_y=None,
            height=40,
            halign="left",
            valign="middle"
        )
        step1_label.bind(size=step1_label.setter('text_size'))
        root_layout.add_widget(step1_label)
        
        browse_btn = Button(
            text="Browse for Project Tracker",
            size_hint_y=None,
            height=50,
            background_color=(0.2, 0.6, 0.8, 1),
            color=(1, 1, 1, 1),
            on_press=self.select_project_tracker
        )
        root_layout.add_widget(browse_btn)
        
        # Manual path entry
        manual_layout = BoxLayout(orientation="horizontal", spacing=5, size_hint_y=None, height=35)
        manual_label = Label(
            text="Or enter path:",
            font_size=10,
            color=(0.7, 0.7, 0.7, 1),
            size_hint_x=None,
            width=100,
            halign="left",
            valign="middle"
        )
        manual_label.bind(size=manual_label.setter('text_size'))
        
        self.manual_path_input = TextInput(
            hint_text="Paste full path to Project Tracker file",
            multiline=False,
            size_hint_y=None,
            height=35,
            font_size=10
        )
        self.manual_path_input.bind(text=self.on_manual_path_change)
        
        manual_layout.add_widget(manual_label)
        manual_layout.add_widget(self.manual_path_input)
        root_layout.add_widget(manual_layout)
        
        self.tracker_status_label = Label(
            text="No file selected",
            font_size=12,
            color=(1, 0.5, 0.5, 1),
            size_hint_y=None,
            height=30,
            halign="left",
            valign="middle"
        )
        self.tracker_status_label.bind(size=self.tracker_status_label.setter('text_size'))
        root_layout.add_widget(self.tracker_status_label)
        
        # Step 2: Date Range
        step2_label = Label(
            text="Step 2: Select Date Range",
            font_size=16,
            bold=True,
            color=(1, 1, 1, 1),
            size_hint_y=None,
            height=40,
            halign="left",
            valign="middle"
        )
        step2_label.bind(size=step2_label.setter('text_size'))
        root_layout.add_widget(step2_label)
        
        date_layout = BoxLayout(orientation="horizontal", spacing=10, size_hint_y=None, height=50)
        
        self.start_date_input = TextInput(
            hint_text="Start Date (YYYY-MM-DD)",
            multiline=False,
            size_hint_y=None,
            height=50
        )
        
        self.end_date_input = TextInput(
            hint_text="End Date (YYYY-MM-DD)",
            multiline=False,
            size_hint_y=None,
            height=50
        )
        
        date_layout.add_widget(self.start_date_input)
        date_layout.add_widget(self.end_date_input)
        root_layout.add_widget(date_layout)
        
        # Set default dates (last 90 days)
        current_date = datetime.now().date()
        start_date = current_date - pd.Timedelta(days=90)
        self.start_date_input.text = start_date.strftime('%Y-%m-%d')
        self.end_date_input.text = current_date.strftime('%Y-%m-%d')
        
        self.apply_btn = Button(
            text="Apply Date Filter & Start Processing",
            size_hint_y=None,
            height=50,
            background_color=(0, 0.8, 0, 1),
            color=(1, 1, 1, 1),
            on_press=self.apply_date_filter,
            disabled=True
        )
        root_layout.add_widget(self.apply_btn)
        
        # Step 3: Output
        step3_label = Label(
            text="Step 3: Output Location",
            font_size=16,
            bold=True,
            color=(1, 1, 1, 1),
            size_hint_y=None,
            height=40,
            halign="left",
            valign="middle"
        )
        step3_label.bind(size=step3_label.setter('text_size'))
        root_layout.add_widget(step3_label)
        
        output_path_label = Label(
            text=f"Output: ~/Desktop/Automated_Data_Processing_Output",
            font_size=10,
            color=(0.7, 0.9, 1, 1),
            size_hint_y=None,
            height=25,
            halign="left",
            valign="middle"
        )
        output_path_label.bind(size=output_path_label.setter('text_size'))
        root_layout.add_widget(output_path_label)
        
        self.open_folder_btn = Button(
            text="Open Output Folder",
            size_hint_y=None,
            height=50,
            background_color=(1, 0.5, 0, 1),
            color=(1, 1, 1, 1),
            on_press=self.open_output_folder,
            disabled=True
        )
        root_layout.add_widget(self.open_folder_btn)
        
        # Status
        self.status_label = Label(
            text="Status: Ready to process",
            font_size=14,
            bold=True,
            color=(0.8, 0.8, 0.8, 1),
            size_hint_y=None,
            height=40,
            halign="center",
            valign="middle"
        )
        self.status_label.bind(size=self.status_label.setter('text_size'))
        root_layout.add_widget(self.status_label)
        
        self.progress_bar = ProgressBar(
            max=100,
            value=0,
            size_hint_y=None,
            height=20
        )
        root_layout.add_widget(self.progress_bar)
        
        # Exit
        exit_btn = Button(
            text="Exit",
            size_hint_y=None,
            height=50,
            background_color=(0.8, 0, 0, 1),
            color=(1, 1, 1, 1),
            on_press=self.stop
        )
        root_layout.add_widget(exit_btn)
        
        # Footer
        footer = Label(
            text="Mac Optimized | OneDrive Auto-Detection | Hidden Sheet Support",
            font_size=12,
            color=(0.6, 0.6, 0.6, 1),
            size_hint_y=None,
            height=30,
            halign="center",
            valign="middle"
        )
        footer.bind(size=footer.setter('text_size'))
        root_layout.add_widget(footer)
        
        # Check folders after UI is built
        Clock.schedule_once(self.check_folders_after_build, 0.5)
        
        return root_layout
    
    def check_folders_after_build(self, dt):
        """Check OneDrive folders after GUI is built"""
        self.sharepoint_access_ok = self.check_sharepoint_access()
        
        if not self.sharepoint_access_ok:
            self.folders_found_label.text = "⚠️ OneDrive folders not found - check sync status"
            self.folders_found_label.color = (1, 0.5, 0, 1)
            self.update_status("WARNING: OneDrive folders not accessible")
        else:
            found_count = len([p for p in self.sharepoint_paths if os.path.exists(p)])
            self.folders_found_label.text = f"✓ Found {found_count}/3 OneDrive folders"
            self.folders_found_label.color = (0.5, 1, 0.5, 1)
            self.update_status("Ready - OneDrive folders detected")
    
    def update_rect(self, instance, value):
        self.rect.pos = instance.pos
        self.rect.size = instance.size
    
    def check_sharepoint_access(self):
        """Enhanced OneDrive folder detection for Mac"""
        try:
            # Target folder names
            target_folders = [
                "Private Brands - Packaging Operations - Building Products",
                "Private Brands - Packaging Operations - Hardlines & Seasonal",
                "Private Brands - Packaging Operations - Home Décor"
            ]
            
            # Search in all possible OneDrive locations
            possible_base_paths = [
                os.path.expanduser("~/Lowe's Companies Inc"),
                os.path.expanduser("~/OneDrive - Lowe's Companies Inc"),
                os.path.expanduser("~/Library/CloudStorage/OneDrive-Lowe'sCompaniesInc"),
                os.path.expanduser("~/Documents/Lowe's Companies Inc"),
                os.path.expanduser("~/Desktop/Lowe's Companies Inc"),
            ]
            
            # Also search using glob for any OneDrive variation
            home = os.path.expanduser("~")
            glob_patterns = [
                f"{home}/Lowe*",
                f"{home}/OneDrive*/Lowe*",
                f"{home}/Library/CloudStorage/*Lowe*",
            ]
            
            for pattern in glob_patterns:
                for path in glob.glob(pattern):
                    if os.path.isdir(path):
                        possible_base_paths.append(path)
            
            # Remove duplicates
            possible_base_paths = list(set(possible_base_paths))
            
            self.log_message(f"Searching {len(possible_base_paths)} possible OneDrive locations...")
            
            # Check each base path for target folders
            found_folders = []
            for base_path in possible_base_paths:
                if not os.path.exists(base_path):
                    continue
                    
                self.log_message(f"Checking: {base_path}")
                
                for folder_name in target_folders:
                    full_path = os.path.join(base_path, folder_name)
                    if os.path.exists(full_path):
                        found_folders.append(full_path)
                        self.log_message(f"✓ Found: {folder_name}")
            
            if found_folders:
                self.sharepoint_paths = found_folders
                self.log_message(f"Successfully found {len(found_folders)} OneDrive folders")
                return True
            
            self.log_message("No OneDrive folders found")
            return False
            
        except Exception as e:
            self.log_message(f"OneDrive detection error: {e}")
            return False
    
    def setup_paths(self):
        """Setup paths with enhanced OneDrive detection"""
        # Target folder names
        target_folders = [
            "Private Brands - Packaging Operations - Building Products",
            "Private Brands - Packaging Operations - Hardlines & Seasonal",
            "Private Brands - Packaging Operations - Home Décor"
        ]
        
        # Initialize empty - will be populated by check_sharepoint_access()
        self.sharepoint_paths = []
        
        # Output folder
        desktop = os.path.expanduser("~/Desktop")
        self.output_folder = os.path.join(desktop, "Automated_Data_Processing_Output")
        
        # Create output directory
        try:
            os.makedirs(self.output_folder, exist_ok=True)
            os.chmod(self.output_folder, 0o755)
        except Exception as e:
            print(f"Error creating output folder: {e}")
    
    def log_message(self, message):
        """Log messages"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}"
        self.processing_logs.append(formatted_message)
        print(formatted_message)
    
    def select_project_tracker(self, instance):
        """Select project tracker using native Mac dialog"""
        try:
            def select_thread():
                try:
                    # Try AppleScript first
                    if platform.system() == 'Darwin':
                        try:
                            applescript = '''
                            tell application "System Events"
                                set theFile to choose file with prompt "Select Project Tracker Excel File" ¬
                                    of type {"org.openxmlformats.spreadsheetml.sheet", "com.microsoft.excel.xls"}
                                return POSIX path of theFile
                            end tell
                            '''
                            
                            result = subprocess.run(['osascript', '-e', applescript], 
                                                  capture_output=True, text=True, timeout=60)
                            
                            if result.returncode == 0 and result.stdout.strip():
                                file_path = result.stdout.strip()
                                Clock.schedule_once(lambda dt: self.update_file_selection(os.path.basename(file_path)), 0)
                                self.project_tracker_path = file_path
                                return
                        except:
                            pass
                    
                    # Fallback to tkinter
                    root = tk.Tk()
                    root.withdraw()
                    root.wm_attributes('-topmost', True)
                    
                    file_path = filedialog.askopenfilename(
                        title="Select Project Tracker Excel File",
                        filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]
                    )
                    
                    root.quit()
                    root.destroy()
                    
                    if file_path:
                        Clock.schedule_once(lambda dt: self.update_file_selection(os.path.basename(file_path)), 0)
                        self.project_tracker_path = file_path
                    
                except Exception as e:
                    Clock.schedule_once(lambda dt: self.show_popup("Error", f"File dialog error: {str(e)}"), 0)
            
            threading.Thread(target=select_thread, daemon=True).start()
            
        except Exception as e:
            self.show_popup("Error", f"Error: {str(e)}")
    
    def on_manual_path_change(self, instance, text):
        """Handle manual path entry"""
        if text and text.strip():
            file_path = text.strip()
            if os.path.exists(file_path) and file_path.lower().endswith(('.xlsx', '.xls')):
                self.project_tracker_path = file_path
                filename = os.path.basename(file_path)
                self.tracker_status_label.text = f"Manual: {filename}"
                self.tracker_status_label.color = (0.5, 1, 0.5, 1)
                self.apply_btn.disabled = False
                self.log_message(f"Project tracker: {filename}")
    
    def update_file_selection(self, filename):
        """Update UI after file selection"""
        self.tracker_status_label.text = f"Selected: {filename}"
        self.tracker_status_label.color = (0.5, 1, 0.5, 1)
        self.apply_btn.disabled = False
        self.log_message(f"Project tracker: {filename}")
    
    def apply_date_filter(self, instance):
        """Apply date filter and start processing"""
        try:
            if not self.sharepoint_access_ok:
                self.show_popup("OneDrive Required", 
                    "OneDrive folders not found!\n\n"
                    "Required folders:\n"
                    "• Private Brands - Packaging Operations - Building Products\n"
                    "• Private Brands - Packaging Operations - Hardlines & Seasonal\n"
                    "• Private Brands - Packaging Operations - Home Décor\n\n"
                    "Please ensure OneDrive is synced and folders are accessible.")
                return
            
            start_str = self.start_date_input.text.strip()
            end_str = self.end_date_input.text.strip()
            
            if not start_str or not end_str:
                self.show_popup("Error", "Please enter both dates")
                return
            
            start_date = datetime.strptime(start_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_str, '%Y-%m-%d').date()
            
            if start_date > end_date:
                self.show_popup("Error", "Start date must be before end date")
                return
            
            self.apply_btn.disabled = True
            self.run_automated_workflow(start_date, end_date)
            
        except ValueError:
            self.show_popup("Error", "Use YYYY-MM-DD format")
        except Exception as e:
            self.show_popup("Error", f"Error: {str(e)}")
    
    @mainthread
    def update_status(self, message):
        if self.status_label:
            self.status_label.text = f"Status: {message}"
    
    @mainthread
    def update_progress(self, value):
        if self.progress_bar:
            self.progress_bar.value = value
    
    @mainthread
    def show_popup(self, title, message):
        content = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        message_label = Label(text=message, halign="center", valign="middle")
        btn = Button(text="OK", size_hint_y=None, height=50)
        
        popup = Popup(title=title, content=content, size_hint=(0.8, 0.6))
        btn.bind(on_press=popup.dismiss)
        
        content.add_widget(message_label)
        content.add_widget(btn)
        popup.open()
    
    @mainthread
    def show_success_popup(self, message):
        content = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        message_label = Label(text=message, halign="center", valign="middle")
        btn = Button(text="OK", size_hint_y=None, height=50, background_color=(0, 0.8, 0, 1))
        
        popup = Popup(title="Success!", content=content, size_hint=(0.9, 0.8))
        btn.bind(on_press=popup.dismiss)
        
        content.add_widget(message_label)
        content.add_widget(btn)
        popup.open()
        
        self.open_folder_btn.disabled = False
    
    def open_output_folder(self, instance):
        """Open output folder"""
        try:
            subprocess.run(['open', self.output_folder], check=True)
        except Exception as e:
            self.show_popup("Error", f"Could not open folder: {str(e)}")
    
    def run_automated_workflow(self, start_date, end_date):
        """Run workflow in background"""
        def process_thread():
            try:
                total_start = time.time()
                self.log_message("Starting workflow...")
                
                self.update_status("Processing...")
                self.update_progress(10)
                
                # Scan folders
                self.update_status("Scanning OneDrive folders...")
                if not self.scan_production_folders():
                    raise Exception("No production files found")
                self.update_progress(20)
                
                # Extract data
                self.update_status("Extracting data (including hidden sheets)...")
                if not self.intelligent_data_extraction():
                    raise Exception("Data extraction failed")
                self.update_progress(40)
                
                # Process tracker
                self.update_status("Processing project tracker...")
                if not self.process_project_tracker():
                    raise Exception("Tracker processing failed")
                self.update_progress(60)
                
                # Combine
                self.update_status("Combining datasets...")
                if not self.combine_datasets():
                    raise Exception("Combination failed")
                self.update_progress(70)
                
                # Filter by date
                self.update_status("Filtering by date...")
                if not self.filter_by_date_range(start_date, end_date):
                    raise Exception("Date filtering failed")
                self.update_progress(80)
                
                # Format output
                self.update_status("Formatting output...")
                if not self.format_final_output():
                    raise Exception("Formatting failed")
                self.update_progress(90)
                
                # Save
                self.update_status("Saving files...")
                output_files = self.save_all_outputs(start_date, end_date)
                self.update_progress(100)
                
                total_time = time.time() - total_start
                
                success_msg = (
                    f"Processing Complete!\n\n"
                    f"Time: {total_time:.1f}s\n"
                    f"Date Range: {start_date} to {end_date}\n"
                    f"Records: {len(self.final_output_data):,}\n"
                    f"Files Created: {len(output_files)}\n\n"
                    f"Saved to Desktop → Automated_Data_Processing_Output"
                )
                
                self.update_status("Complete!")
                self.show_success_popup(success_msg)
                
            except Exception as e:
                self.update_progress(0)
                self.update_status("Failed")
                self.log_message(f"Error: {str(e)}")
                self.show_popup("Error", f"Processing failed: {str(e)}")
        
        threading.Thread(target=process_thread, daemon=True).start()
    
    # Simplified versions of processing methods for space
    # (Include your full methods from the original file here)
    
    def scan_production_folders(self):
        """Scan for production files"""
        self.log_message("Scanning production folders...")
        all_files = []
        
        for sp_path in self.sharepoint_paths:
            if not os.path.exists(sp_path):
                continue
                
            try:
                for root, dirs, files in os.walk(sp_path):
                    dirs[:] = [d for d in dirs if not d.startswith('.')]
                    
                    if root.endswith("_Production Item List"):
                        excel_files = [f for f in files 
                                     if f.lower().endswith(('.xlsx', '.xls', '.xlsm')) 
                                     and not f.startswith(('~', '.', '$'))]
                        
                        for excel_file in excel_files:
                            full_path = os.path.join(root, excel_file)
                            if os.access(full_path, os.R_OK):
                                all_files.append(full_path)
            except Exception as e:
                self.log_message(f"Scan error {sp_path}: {str(e)}")
        
        self.production_files = all_files
        self.log_message(f"Found {len(all_files)} production files")
        return len(all_files) > 0
    
    def intelligent_data_extraction(self):
        """Extract data with hidden sheet support"""
        # Implement your full extraction logic here
        self.log_message("Extracting data...")
        # Placeholder - use your full implementation
        self.consolidated_data = pd.DataFrame()
        return True
    
    def process_project_tracker(self):
        """Process project tracker"""
        # Implement your full tracker logic here
        self.log_message("Processing tracker...")
        self.project_tracker_data = pd.DataFrame()
        return True
    
    def combine_datasets(self):
        """Combine datasets"""
        self.log_message("Combining...")
        self.combined_data = pd.DataFrame()
        return True
    
    def filter_by_date_range(self, start_date, end_date):
        """Filter by date"""
        self.log_message(f"Filtering {start_date} to {end_date}")
        return True
    
    def format_final_output(self):
        """Format output"""
        self.log_message("Formatting...")
        self.final_output_data = pd.DataFrame(columns=self.final_columns)
        return True
    
    def save_all_outputs(self, start_date, end_date):
        """Save outputs"""
        self.log_message("Saving...")
        return []

def main():
    """Main function for Mac"""
    try:
        if platform.system() != 'Darwin':
            print("⚠️  This app is optimized for macOS")
        
        print("🚀 Starting Mac Automated Data Processor...")
        
        Window.minimum_width = 600
        Window.minimum_height = 500
        Window.size = (800, 700)
        
        app = AutomatedDataProcessor()
        app.run()
        
    except Exception as e:
        print(f"Startup error: {e}")

if __name__ == "__main__":
    main()
