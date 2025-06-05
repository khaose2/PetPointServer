import logging
import sqlite3
import threading
import os
import configparser # For saving/loading settings
from datetime import datetime, timezone
from io import BytesIO
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import STATE_RUNNING # For checking scheduler state

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from PIL import Image, ImageTk

import gspread
from ttkthemes import ThemedTk

# --- Application Constants ---
APP_VERSION = "1.3.5" 
APP_AUTHOR = "Jeff H" 

# --- Configuration File ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE_PATH = os.path.join(SCRIPT_DIR, 'settings.ini')

# --- Default Configuration Values ---
DEFAULT_SETTINGS = {
    'theme': 'arc', 
    'log_level': 'INFO',
    'sync_archived_to_gsheet': 'true',
    'last_successful_sync_all': 'N/A',
    'last_successful_gsheet_update': 'N/A'
}

# --- Petango Configuration ---
PETANGO_URL = (
    'https://ws.petango.com/webservices/adoptablesearch/wsAdoptableAnimals2.aspx'
    '?species=All&gender=A&agegroup=All&location=&site=&onhold=A&orderby=Name'
    '&colnum=4&css=&authkey=40fm1dbi1t4267edhjlafrfmbgfqfvmi0vjjm3iori7pxqk8xp'
    '&recAmount=&detailsInPopup=Yes&featuredPet=Include&stageID='
)

# --- Local Database Configuration ---
DB_PATH = os.path.join(SCRIPT_DIR, 'pets_enhanced.db')

# --- Google Sheets Configuration ---
GOOGLE_CREDENTIALS_FILE = os.path.join(SCRIPT_DIR, 'sacred-sol-346504-70a7b5193a92.json')
GOOGLE_SHEET_ID = '1jO9GowDUU6GzSHA39zbR7beK3GgW9bAsY71LIftdCyM'
GOOGLE_SHEET_FALLBACK_NAME = 'Pets'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file']

# --- Logging Configuration ---
LOG_BUFFER_MAX_LINES = 500

# --- Logging Setup ---
log_buffer: List[str] = []
class TkLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord):
        log_entry = self.format(record)
        log_buffer.append(log_entry)
        if len(log_buffer) > LOG_BUFFER_MAX_LINES:
            log_buffer.pop(0)

tk_log_handler = TkLogHandler()
tk_log_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S'))
logger = logging.getLogger("pet_sync_gui")
logger.addHandler(tk_log_handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s'))
logger.addHandler(console_handler)

# --- Configuration Functions ---
def load_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser(defaults=DEFAULT_SETTINGS)
    if os.path.exists(CONFIG_FILE_PATH):
        try:
            config.read(CONFIG_FILE_PATH)
            if 'AppSettings' not in config:
                config.add_section('AppSettings')
            for key, value in DEFAULT_SETTINGS.items():
                if not config.has_option('AppSettings', key):
                    config.set('AppSettings', key, value)
        except configparser.Error as e:
            logger.error(f"Error reading config file {CONFIG_FILE_PATH}: {e}. Using defaults.")
            config = configparser.ConfigParser(defaults=DEFAULT_SETTINGS)
            config.add_section('AppSettings') 
    else:
        logger.info(f"Config file {CONFIG_FILE_PATH} not found. Creating with default settings.")
        if 'AppSettings' not in config:
            config.add_section('AppSettings')
        for key, value in DEFAULT_SETTINGS.items():
            if not config.has_option('AppSettings', key):
                 config.set('AppSettings', key, value)
        with open(CONFIG_FILE_PATH, 'w') as configfile:
            config.write(configfile)
    return config

def save_config(config: configparser.ConfigParser):
    try:
        with open(CONFIG_FILE_PATH, 'w') as configfile:
            config.write(configfile)
        logger.info(f"Configuration saved to {CONFIG_FILE_PATH}")
    except IOError as e:
        logger.error(f"Error saving config file {CONFIG_FILE_PATH}: {e}", exc_info=True)

# --- Database Initialization ---
def init_db():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS pets (
                    id TEXT PRIMARY KEY, name TEXT, species TEXT, breed TEXT,
                    sexSN TEXT, age TEXT, location TEXT, detail_id TEXT,
                    photo_url TEXT, last_seen TIMESTAMP, archived INTEGER DEFAULT 0
                )
                '''
            )
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_last_seen ON pets(last_seen)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_archived_name ON pets(archived, name)")
            conn.commit()
        logger.info('Successfully initialized database at %s', DB_PATH)
    except sqlite3.Error as e:
        logger.error('Database initialization failed: %s', e, exc_info=True)
        raise

# --- Fetch & Parse Petango Data ---
def fetch_animals() -> List[Dict[str, Any]]:
    logger.info('Fetching pet data from Petango...')
    animals: List[Dict[str, Any]] = []
    try:
        resp = requests.get(PETANGO_URL, timeout=30)
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        logger.error('Failed to fetch data from Petango: Request timed out.')
        return animals
    except requests.exceptions.RequestException as e:
        logger.error('Failed to fetch data from Petango: %s', e)
        return animals
    soup = BeautifulSoup(resp.text, 'html.parser')
    for item_idx, item in enumerate(soup.find_all('div', class_='list-item')):
        try:
            info = item.find('div', class_='list-animal-info-block')
            photo_block = item.find('div', class_='list-animal-photo-block')

            def get_text_from_class(element, class_name):
                found = element.find('div', class_=class_name) if element else None
                return found.text.strip() if found and found.text else None

            pet_id = get_text_from_class(info, 'list-animal-id')
            if not pet_id:
                logger.warning(f"Skipping item {item_idx+1} due to missing Pet ID.")
                continue
            
            if item_idx < 3 and logger.isEnabledFor(logging.DEBUG): 
                 logger.debug(f"Item {item_idx+1}, Pet ID {pet_id}: Raw info block HTML: {info.prettify() if info else 'N/A'}")

            parsed_species = get_text_from_class(info, 'list-anima-species') 
            if logger.isEnabledFor(logging.DEBUG): 
                logger.debug(f"Item {item_idx+1}, Pet ID {pet_id}: Attempt 1 (class 'list-anima-species') - Parsed species: '{parsed_species}'")

            if not parsed_species and info:
                logger.debug(f"Item {item_idx+1}, Pet ID {pet_id}: Species not found with specific class 'list-anima-species'. Trying alternative label search.")
                species_label_tag = info.find(lambda tag: tag.name == 'div' and "Species:" in tag.get_text(strip=True))
                if species_label_tag:
                    logger.debug(f"Item {item_idx+1}, Pet ID {pet_id}: Found label tag: {species_label_tag.prettify()[:100]}")
                    value_tag = species_label_tag.find_next_sibling('div')
                    if value_tag:
                        parsed_species = value_tag.get_text(strip=True)
                        logger.debug(f"Item {item_idx+1}, Pet ID {pet_id}: Attempt 2 (next sibling) - Parsed species: '{parsed_species}'")
                    else: 
                        parent_row = species_label_tag.find_parent(class_='list-animal-row') 
                        if parent_row:
                             value_in_row = parent_row.find('div', class_='list-animal-value')
                             if value_in_row:
                                 parsed_species = value_in_row.get_text(strip=True)
                                 logger.debug(f"Item {item_idx+1}, Pet ID {pet_id}: Attempt 2b (value in row) - Parsed species: '{parsed_species}'")
                else:
                    logger.debug(f"Item {item_idx+1}, Pet ID {pet_id}: Could not find 'Species:' label for alternative parsing.")
            
            if not parsed_species:
                 logger.warning(f"Item {item_idx+1}, Pet ID {pet_id}: Species could not be parsed. Will be stored as None/empty.")


            animal = {
                'id': pet_id, 'name': get_text_from_class(info, 'list-animal-name'),
                'species': parsed_species, 
                'breed': get_text_from_class(info, 'list-animal-breed'),
                'sexSN': get_text_from_class(info, 'list-animal-sexSN'),
                'age': get_text_from_class(info, 'list-animal-age'),
                'location': get_text_from_class(info, 'hidden'), 
                'detail_id': get_text_from_class(info, 'list-animal-detail'), 'photo_url': None
            }
            photo_img = photo_block.find('img') if photo_block else None
            if photo_img and photo_img.get('src'):
                animal['photo_url'] = photo_img['src']
            animals.append(animal)
        except Exception as e:
            logger.error(f"Error parsing individual animal item {item_idx+1}: %s. Item content: %s", e, item.prettify()[:200], exc_info=True)
    logger.info('Parsed %d animals from Petango.', len(animals))
    return animals

# --- Synchronization Logic (Local DB) ---
def sync_database():
    sync_time = datetime.now(timezone.utc)
    animals_fetched = fetch_animals()
    if not animals_fetched:
        logger.warning("No animals fetched from Petango (first attempt). Retrying once.")
        animals_fetched = fetch_animals()
    if not animals_fetched:
        logger.error("Failed to fetch animal data from Petango after retry. Skipping database sync.")
        raise ConnectionError("Failed to fetch animal data from Petango after retry.")
    updated_count = 0; inserted_count = 0
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            active_ids = {a['id'] for a in animals_fetched if a.get('id')}
            for a in animals_fetched:
                if not a.get('id'): continue
                cursor.execute("SELECT id FROM pets WHERE id = ?", (a['id'],))
                exists = cursor.fetchone()
                pet_details = (
                    a.get('name'), a.get('species'), a.get('breed'), a.get('sexSN'), a.get('age'),
                    a.get('location'), a.get('detail_id'), a.get('photo_url'), sync_time
                )
                if exists:
                    cursor.execute(
                        '''UPDATE pets SET name = ?, species = ?, breed = ?, sexSN = ?, age = ?,
                           location = ?, detail_id = ?, photo_url = ?, last_seen = ?, archived = 0
                           WHERE id = ?''',
                        (*pet_details, a['id']))
                    if cursor.rowcount > 0: updated_count +=1
                else:
                    cursor.execute(
                        '''INSERT INTO pets (id, name, species, breed, sexSN, age, location,
                           detail_id, photo_url, last_seen, archived)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)''',
                        (a['id'], *pet_details))
                    if cursor.rowcount > 0: inserted_count +=1
            archived_count = 0
            if active_ids: 
                placeholders = ', '.join('?' for _ in active_ids)
                archive_sql = f"UPDATE pets SET archived = 1, last_seen = ? WHERE id NOT IN ({placeholders}) AND archived = 0"
                archived_pets_params = [sync_time] + list(active_ids)
                cursor.execute(archive_sql, archived_pets_params)
                archived_count = cursor.rowcount
            else:
                logger.warning("Skipping archival step as no active animal IDs were processed from fetch (animals_fetched was empty).")
            conn.commit()
            logger.info('Local DB sync complete. Inserted: %d, Updated: %d, Archived: %d.',
                        inserted_count, updated_count, archived_count)
    except sqlite3.Error as e: logger.error('Database error during local sync: %s', e, exc_info=True); raise
    except ConnectionError: raise
    except Exception as e: logger.error('Unexpected error during local sync: %s', e, exc_info=True); raise

# --- Filtered Data Fetching ---
def fetch_filtered_pets_from_db(app_config: configparser.ConfigParser,
                                search_term: str = "",
                                species_filter: str = "All",
                                sex_filter: str = "All",
                                archived_filter: str = "Active") -> List[Dict[str, Any]]:
    logger.debug(f"Fetching filtered pets. Search: '{search_term}', Species: '{species_filter}', Sex: '{sex_filter}', Archived: '{archived_filter}'")
    pets = []
    query = f"SELECT {', '.join(PetSyncGUI.COLUMNS)} FROM pets" 
    conditions = []
    params = []

    if search_term:
        search_like = f"%{search_term}%"
        conditions.append("(LOWER(id) LIKE LOWER(?) OR LOWER(name) LIKE LOWER(?) OR LOWER(species) LIKE LOWER(?) OR LOWER(breed) LIKE LOWER(?))")
        params.extend([search_like] * 4)
    if species_filter != "All" and species_filter:
        conditions.append("LOWER(species) = LOWER(?)")
        params.append(species_filter)
    if sex_filter != "All" and sex_filter:
        conditions.append("LOWER(sexSN) = LOWER(?)")
        params.append(sex_filter)
    if archived_filter == "Active":
        conditions.append("archived = 0")
    elif archived_filter == "Archived":
        conditions.append("archived = 1")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY archived ASC, name ASC"
    
    logger.debug(f"Executing query: {query} with params: {params}")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query, params)
            pets = [dict(row) for row in cursor.fetchall()]
        logger.info(f"Fetched {len(pets)} pets from local DB based on filters.")
    except sqlite3.Error as e:
        logger.error(f"Error fetching filtered pets from local DB: {e}", exc_info=True)
    return pets

# --- Google Sheets Update Functions ---
def update_google_sheet(pets_data: List[Dict[str, Any]]) -> bool:
    logger.info(f"Entering update_google_sheet function with {len(pets_data)} pets.")
    if not GOOGLE_SHEET_ID or GOOGLE_SHEET_ID == 'your_sheet_id_here':
        logger.error("Google Sheet ID is not configured. Please update GOOGLE_SHEET_ID constant.")
        return False
    if not pets_data:
        logger.info("No pet data provided to update_google_sheet. Skipping actual sheet update.")
        return True 
    logger.debug(f"Using GSheet ID: {GOOGLE_SHEET_ID}, Credentials file: {GOOGLE_CREDENTIALS_FILE}")
    try:
        logger.debug("Attempting to authenticate with Google service account...")
        gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE, scopes=SCOPES)
        logger.info("Successfully authenticated with Google service account.")
        logger.debug(f"Attempting to open Google Sheet by ID: {GOOGLE_SHEET_ID}")
        sh = gc.open_by_key(GOOGLE_SHEET_ID).sheet1
        logger.info(f"Successfully opened Google Sheet: '{sh.title}' (ID: {GOOGLE_SHEET_ID})")
        headers = ['ID', 'Name', 'Species', 'Breed', 'Sex/SN', 'Age', 'Location', 'Detail ID', 'Photo URL', 'Archived']
        rows_to_write = [headers]
        for pet in pets_data:
            row = [
                pet.get('id', ''), pet.get('name', ''), pet.get('species', ''),
                pet.get('breed', ''), pet.get('sexSN', ''), pet.get('age', ''),
                pet.get('location', ''), pet.get('detail_id', ''),
                pet.get('photo_url', ''), 'Yes' if pet.get('archived', 0) == 1 else 'No'
            ]
            rows_to_write.append(row)
        logger.debug(f"Preparing to write {len(rows_to_write)} rows (including header) to sheet '{sh.title}'.")
        sh.clear()
        logger.debug("Sheet cleared. Updating with new data...")
        sh.update('A1', rows_to_write, value_input_option='USER_ENTERED')
        logger.debug("Sheet data updated. Formatting header...")
        sh.format('A1:J1', {'textFormat': {'bold': True}})
        logger.info(f"Google Sheet (ID: {GOOGLE_SHEET_ID}) updated successfully with {len(pets_data)} pets.")
        return True
    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Google Sheet with ID '{GOOGLE_SHEET_ID}' not found. "
                     "Ensure the ID is correct and the sheet is shared with the service account.", exc_info=True)
    except gspread.exceptions.APIError as e:
        error_details = e.response.json() if hasattr(e.response, 'json') else str(e)
        logger.error(f"Google Sheets API Error for sheet ID {GOOGLE_SHEET_ID}: {error_details}", exc_info=True)
    except FileNotFoundError:
        logger.error(f"Google credentials file ('{GOOGLE_CREDENTIALS_FILE}') not found. "
                     "Please ensure the file exists at this path and the script has permissions to read it.", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred while updating Google Sheet (ID: {GOOGLE_SHEET_ID}): {e}", exc_info=True)
    return False

# --- GUI Section ---
class PetSyncGUI:
    COLUMNS = ('id', 'name', 'species', 'breed', 'sexSN', 'age', 'location', 'detail_id', 'photo_url', 'archived')
    SPECIES_OPTIONS = ["All", "Dog", "Cat"] # UPDATED
    SEX_OPTIONS = ["All", "Male", "Female", "Neutered Male", "Spayed Female", "Unknown"] 
    ARCHIVED_OPTIONS = ["Active", "Archived", "All"]

    def __init__(self, master: ThemedTk, initial_config: configparser.ConfigParser):
        self.master = master 
        self.config = initial_config
        
        self.master.title(f"Petango Sync v{APP_VERSION}")
        self.master.geometry("1250x800")
        
        self.scheduler = BackgroundScheduler(timezone="UTC")
        self.sync_running = False
        self._current_pet_photo: Optional[ImageTk.PhotoImage] = None
        
        self.current_theme_var = tk.StringVar(value=self.config.get('AppSettings', 'theme'))
        self.log_level_var = tk.StringVar(value=self.config.get('AppSettings', 'log_level'))
        self.sync_archived_to_gsheet_var = tk.BooleanVar(value=self.config.getboolean('AppSettings', 'sync_archived_to_gsheet'))

        self.search_term_var = tk.StringVar()
        self.species_filter_var = tk.StringVar(value="All")
        self.sex_filter_var = tk.StringVar(value="All")
        self.archived_filter_var = tk.StringVar(value="Active")

        self._apply_log_level() 

        self._setup_menubar()
        self._setup_ui() 
        self.master.protocol("WM_DELETE_WINDOW", self._on_closing)
        self.master.after(100, self.refresh_pet_table) 
        self.update_button_states()
        self._update_last_synced_labels()

    def _apply_log_level(self):
        level_str = self.log_level_var.get()
        logger.setLevel(logging.DEBUG if level_str == "DEBUG" else logging.INFO)
        # Log is done by caller

    def _save_app_setting(self, key: str, value: Any):
        self.config.set('AppSettings', key, str(value)); save_config(self.config)

    def _setup_menubar(self):
        menubar = tk.Menu(self.master); self.master.config(menu=menubar)
        settings_menu = tk.Menu(menubar, tearoff=0); menubar.add_cascade(label="Settings", menu=settings_menu)
        theme_menu = tk.Menu(settings_menu, tearoff=0); settings_menu.add_cascade(label="Themes", menu=theme_menu)
        try: 
            available_themes = sorted(self.master.get_themes())
            if not available_themes: raise tk.TclError("No themes from get_themes")
        except (AttributeError, tk.TclError): 
            style = ttk.Style(); available_themes = sorted(style.theme_names())
        
        current_theme_val = self.current_theme_var.get()
        if current_theme_val not in available_themes and available_themes:
            self.current_theme_var.set(available_themes[0])
        elif not available_themes:
             self.current_theme_var.set("default") 
             available_themes.append("default")

        for theme_name in available_themes: 
            theme_menu.add_radiobutton(label=theme_name.capitalize(), variable=self.current_theme_var, value=theme_name, command=self._on_theme_change)
        
        gsheet_options_menu = tk.Menu(settings_menu, tearoff=0); settings_menu.add_cascade(label="Google Sheet Options", menu=gsheet_options_menu)
        gsheet_options_menu.add_checkbutton(label="Sync Archived Pets to Sheet", variable=self.sync_archived_to_gsheet_var, command=self._on_sync_archived_setting_change)
        
        log_level_menu = tk.Menu(settings_menu, tearoff=0); settings_menu.add_cascade(label="Log Level", menu=log_level_menu)
        for level_name in ["INFO", "DEBUG"]: 
            log_level_menu.add_radiobutton(label=level_name, variable=self.log_level_var, value=level_name, command=self._on_log_level_change)
        
        settings_menu.add_separator(); settings_menu.add_command(label="Exit", command=self._on_closing)
        help_menu = tk.Menu(menubar, tearoff=0); menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="About", command=self._show_about_dialog)

    def _on_theme_change(self):
        new_theme = self.current_theme_var.get()
        try: 
            logger.info(f"Attempting to change theme to: {new_theme}")
            if hasattr(self.master, 'set_theme'): 
                self.master.set_theme(new_theme)
            else: 
                style = ttk.Style(self.master)
                style.theme_use(new_theme)
            self._save_app_setting('theme', new_theme)
            logger.info(f"Theme changed successfully to: {new_theme}")
        except tk.TclError as e_ttk: 
            logger.error(f"Failed to set theme '{new_theme}': {e_ttk}", exc_info=True)
            messagebox.showerror("Theme Error", f"Could not apply theme: {new_theme}\n{e_ttk}")
            self.current_theme_var.set(self.config.get('AppSettings','theme'))

    def _on_sync_archived_setting_change(self):
        is_enabled = self.sync_archived_to_gsheet_var.get()
        self._save_app_setting('sync_archived_to_gsheet', is_enabled)
        logger.info(f"Setting 'Sync Archived Pets to Google Sheet' changed to: {is_enabled}")

    def _on_log_level_change(self):
        new_level_str = self.log_level_var.get()
        self._apply_log_level() 
        self._save_app_setting('log_level', new_level_str)
        logger.info(f"Log level explicitly changed to: {new_level_str} via menu.") 
    
    def _show_about_dialog(self):
        messagebox.showinfo("About Petango Sync", f"Petango Sync Utility\n\nVersion: {APP_VERSION}\nAuthor: {APP_AUTHOR}\n\nThis tool syncs pet data from Petango.")

    def _update_last_synced_labels(self):
        last_sync_all_str = self.config.get('AppSettings', 'last_successful_sync_all', fallback='N/A')
        last_gsheet_str = self.config.get('AppSettings', 'last_successful_gsheet_update', fallback='N/A')
        if hasattr(self, 'last_sync_all_label'): self.last_sync_all_label.config(text=f"Last Full Sync: {last_sync_all_str}")
        if hasattr(self, 'last_gsheet_update_label'): self.last_gsheet_update_label.config(text=f"Last GSheet Update: {last_gsheet_str}")

    def _setup_ui(self):
        toolbar = ttk.Frame(self.master, padding=(5, 5, 5, 0)) 
        toolbar.pack(side='top', fill='x')
        self.btn_start_sync = ttk.Button(toolbar, text="Start Auto Sync", command=self.start_auto_sync, width=15)
        self.btn_start_sync.pack(side='left', padx=(0,3), pady=2)
        self.btn_stop_sync = ttk.Button(toolbar, text="Stop Auto Sync", command=self.stop_auto_sync, width=15)
        self.btn_stop_sync.pack(side='left', padx=3, pady=2)
        self.btn_manual_sync_all = ttk.Button(toolbar, text="Manual Sync All", command=self.trigger_manual_sync_all, width=15)
        self.btn_manual_sync_all.pack(side='left', padx=3, pady=2)
        self.btn_manual_gsheet_sync = ttk.Button(toolbar, text="Update GSheet Only", command=self.trigger_manual_gsheet_update, width=18)
        self.btn_manual_gsheet_sync.pack(side='left', padx=3, pady=2)
        self.btn_show_log = ttk.Button(toolbar, text="Show Debug Log", command=self.show_debug_log, width=15)
        self.btn_show_log.pack(side='left', padx=3, pady=2)
        
        filter_bar = ttk.Frame(self.master, padding=(5,5,5,5))
        filter_bar.pack(side='top', fill='x')
        ttk.Label(filter_bar, text="Search:").pack(side='left', padx=(0,2))
        search_entry = ttk.Entry(filter_bar, textvariable=self.search_term_var, width=20)
        search_entry.pack(side='left', padx=(0,5))
        search_entry.bind("<Return>", lambda event: self._apply_filters_command())
        ttk.Label(filter_bar, text="Species:").pack(side='left', padx=(5,2))
        species_combo = ttk.Combobox(filter_bar, textvariable=self.species_filter_var, values=self.SPECIES_OPTIONS, width=10, state="readonly")
        species_combo.pack(side='left', padx=(0,5)); species_combo.set("All")
        ttk.Label(filter_bar, text="Sex:").pack(side='left', padx=(5,2))
        sex_combo = ttk.Combobox(filter_bar, textvariable=self.sex_filter_var, values=self.SEX_OPTIONS, width=15, state="readonly")
        sex_combo.pack(side='left', padx=(0,5)); sex_combo.set("All")
        ttk.Label(filter_bar, text="Status:").pack(side='left', padx=(5,2))
        archived_combo = ttk.Combobox(filter_bar, textvariable=self.archived_filter_var, values=self.ARCHIVED_OPTIONS, width=10, state="readonly")
        archived_combo.pack(side='left', padx=(0,5)); archived_combo.set("Active")
        apply_button = ttk.Button(filter_bar, text="Apply Filters", command=self._apply_filters_command, width=12)
        apply_button.pack(side='left', padx=5)
        clear_button = ttk.Button(filter_bar, text="Clear Filters", command=self._clear_filters_command, width=12)
        clear_button.pack(side='left', padx=(0,5))

        main_content_frame = ttk.Frame(self.master, padding=(5,0,5,5))
        main_content_frame.pack(side='top', fill='both', expand=True)
        tree_container_frame = ttk.Frame(main_content_frame)
        tree_container_frame.pack(side='left', fill='both', expand=True, padx=(0,5))
        self.pet_tree = ttk.Treeview(tree_container_frame, columns=self.COLUMNS, show='headings', height=18)
        for col in self.COLUMNS:
            self.pet_tree.heading(col, text=col.replace('_', ' ').capitalize(), command=lambda _col=col: self._sort_column(_col, False))
            self.pet_tree.column(col, width=100, anchor="w", minwidth=60)
        self.pet_tree.column('name', width=150, minwidth=100); self.pet_tree.column('breed', width=180, minwidth=120)
        self.pet_tree.column('photo_url', width=200, minwidth=150); self.pet_tree.column('archived', width=70, anchor='center', minwidth=50)
        vsb = ttk.Scrollbar(tree_container_frame, orient="vertical", command=self.pet_tree.yview)
        hsb = ttk.Scrollbar(tree_container_frame, orient="horizontal", command=self.pet_tree.xview)
        self.pet_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side='right', fill='y'); hsb.pack(side='bottom', fill='x')
        self.pet_tree.pack(side='left', fill='both', expand=True)
        self.pet_tree.bind("<<TreeviewSelect>>", self._on_pet_select)
        photo_viewer_frame = ttk.Frame(main_content_frame, width=270, padding=5)
        photo_viewer_frame.pack(side='right', fill='y'); photo_viewer_frame.pack_propagate(False)
        tk.Label(photo_viewer_frame, text="Pet Photo", font=("Arial", 12, "bold")).pack(pady=(0,5))
        self.photo_label = ttk.Label(photo_viewer_frame, text="Select a pet to see photo", anchor=tk.CENTER, relief=tk.GROOVE, borderwidth=1)
        self.photo_label.pack(pady=5, fill=tk.BOTH, expand=True); self.photo_label.image = None
        
        status_bar_frame = ttk.Frame(self.master, padding=(5,2))
        status_bar_frame.pack(side='bottom', fill='x')
        self.last_sync_all_label = ttk.Label(status_bar_frame, text="Last Full Sync: N/A")
        self.last_sync_all_label.pack(side='left', padx=(0,10))
        self.last_gsheet_update_label = ttk.Label(status_bar_frame, text="Last GSheet Update: N/A")
        self.last_gsheet_update_label.pack(side='left')
        self.status_label = ttk.Label(status_bar_frame, text="Ready.", relief=tk.FLAT) 
        self.status_label.pack(side='right', fill='x', expand=True, padx=(10,0))

    def update_button_states(self, operation_running=False):
        is_auto_syncing = self.sync_running
        self.btn_start_sync.config(state='disabled' if is_auto_syncing or operation_running else 'normal')
        self.btn_stop_sync.config(state='normal' if is_auto_syncing and not operation_running else 'disabled')
        self.btn_manual_sync_all.config(state='disabled' if is_auto_syncing or operation_running else 'normal')
        self.btn_manual_gsheet_sync.config(state='disabled' if is_auto_syncing or operation_running else 'normal')
    
    def _update_status(self, message: str, is_error: bool = False):
        if self.master.winfo_exists():
            self.status_label.config(text=message) 
            if is_error:
                logger.error(f"GUI Status Update (Error): {message}")
                self.status_label.config(foreground="red") 
            else:
                logger.info(f"GUI Status Update: {message}")
                self.status_label.config(foreground="") 
    
    def _sort_column(self, col: str, reverse: bool):
        try:
            data = [(self.pet_tree.set(child, col), child) for child in self.pet_tree.get_children('')]
            def sort_key(item):
                try: return int(item[0])
                except ValueError: return str(item[0]).lower()
            data.sort(key=sort_key, reverse=reverse)
            for index, (val, child) in enumerate(data): self.pet_tree.move(child, '', index)
            self.pet_tree.heading(col, command=lambda _col=col: self._sort_column(_col, not reverse))
        except Exception as e: logger.error("Error sorting column %s: %s", col, e, exc_info=True); self._update_status(f"Error sorting: {e}", True)

    def _apply_filters_command(self):
        logger.info("Apply filters/search command triggered.")
        self.refresh_pet_table()

    def _clear_filters_command(self):
        logger.info("Clear filters command triggered.")
        self.search_term_var.set("")
        self.species_filter_var.set("All")
        self.sex_filter_var.set("All")
        self.archived_filter_var.set("Active")
        self.refresh_pet_table()

    def start_auto_sync(self):
        if not self.sync_running:
            try:
                self.sync_running = True 
                self.update_button_states()
                self._update_status("Attempting to start automatic hourly sync...")
                self.trigger_manual_sync_all(scheduled_job=True) 
                self.scheduler.add_job(self._scheduled_sync_all_task, 'interval', hours=1, id='pet_sync_job')
                if self.scheduler.state != STATE_RUNNING: self.scheduler.start(paused=False) 
                self._update_status("Automatic hourly sync (DB & Google Sheet) configured.")
            except Exception as e:
                self.sync_running = False 
                logger.error("Failed to start scheduler: %s", e, exc_info=True)
                self._update_status(f"Error starting scheduler: {e}", True); messagebox.showerror("Scheduler Error", f"Could not start: {e}")
            finally: self.update_button_states() 

    def stop_auto_sync(self):
        if self.sync_running:
            try:
                if self.scheduler.get_job('pet_sync_job'): self.scheduler.remove_job('pet_sync_job')
                self.sync_running = False
                self._update_status("Automatic sync stopped.")
            except Exception as e: logger.error("Failed to stop scheduler: %s", e, exc_info=True); self._update_status(f"Error stopping scheduler: {e}", True)
            finally: self.update_button_states()

    def _scheduled_sync_all_task(self):
        logger.info("Scheduled sync task (DB & Google Sheet) initiated by APScheduler.")
        threading.Thread(target=self._perform_sync_and_update_all, args=("Scheduled sync",), daemon=True).start()

    def trigger_manual_sync_all(self, scheduled_job: bool = False):
        sync_type = "Initial auto-sync" if scheduled_job else "Manual Full Sync"
        self._update_status(f"{sync_type} (DB & Google Sheet) requested...")
        self.update_button_states(operation_running=True)
        threading.Thread(target=self._perform_sync_and_update_all, args=(sync_type,), daemon=True).start()

    def _perform_sync_and_update_all(self, sync_type_msg: str):
        full_task_name = f"{sync_type_msg} (DB & GSheet)"
        self.master.after(0, lambda: self._update_status(f"{full_task_name} in progress..."))
        db_sync_ok = False; gsheet_update_ok = False; any_pets_for_sheet = False
        current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            logger.info(f"Starting: {full_task_name}")
            sync_database(); db_sync_ok = True
            logger.info("Local DB sync successful for %s. Preparing Google Sheet update.", sync_type_msg)
            
            all_pets_from_db = fetch_filtered_pets_from_db(self.config, archived_filter="All")
            any_pets_for_sheet = bool(all_pets_from_db)

            if any_pets_for_sheet:
                if self.config.getboolean('AppSettings', 'sync_archived_to_gsheet'):
                    pets_to_send_to_gsheet = all_pets_from_db
                else:
                    pets_to_send_to_gsheet = [p for p in all_pets_from_db if not p.get('archived')]
                
                logger.info("Calling update_google_sheet for %s with %d pets...", sync_type_msg, len(pets_to_send_to_gsheet))
                if pets_to_send_to_gsheet: 
                    gsheet_update_ok = update_google_sheet(pets_to_send_to_gsheet)
                else: 
                    logger.info("No pets to send to GSheet after applying 'sync_archived_to_gsheet' setting.")
                    gsheet_update_ok = True 
                logger.info(f"update_google_sheet call completed for {sync_type_msg}. Success: {gsheet_update_ok}")
            else:
                logger.info("No pets in local DB to update GSheet with for %s. GSheet update effectively skipped.", sync_type_msg)
                gsheet_update_ok = True 
            
            final_status_is_error = False
            status_msg = f"{full_task_name} complete at {datetime.now().strftime('%H:%M:%S')}."
            if not db_sync_ok: status_msg = f"{full_task_name} failed (DB sync error - check logs)."; final_status_is_error = True
            elif db_sync_ok and any_pets_for_sheet and not gsheet_update_ok: status_msg += " (DB OK, GSheet update FAILED - check logs)"; final_status_is_error = True
            
            if db_sync_ok and gsheet_update_ok:
                self._save_app_setting('last_successful_sync_all', current_time_str)
                self._save_app_setting('last_successful_gsheet_update', current_time_str)
                self.master.after(0, self._update_last_synced_labels)

            self.master.after(0, lambda: self._update_status(status_msg, final_status_is_error))
            if db_sync_ok: self.master.after(0, self.refresh_pet_table)
        except ConnectionError as e: logger.error(f"Connection error during {full_task_name}: {e}", exc_info=True); self.master.after(0, lambda: self._update_status(f"{full_task_name} (Petango fetch) FAILED: {e}", True)); self.master.after(0, lambda: messagebox.showerror("Sync Error", f"Could not fetch Petango data: {e}"))
        except sqlite3.Error as e: logger.error(f"SQLite error during {full_task_name}: {e}", exc_info=True); self.master.after(0, lambda: self._update_status(f"{full_task_name} (local DB) FAILED: {e}", True)); self.master.after(0, lambda: messagebox.showerror("Sync Error", f"Local database operation failed: {e}"))
        except Exception as e:
            logger.error(f"Unexpected error during {full_task_name}: {e}", exc_info=True)
            err_msg = f"{full_task_name} FAILED: {e}"
            if db_sync_ok: err_msg = f"Local DB sync OK for {sync_type_msg}, but subsequent GSheet update/other error: {e}"
            self.master.after(0, lambda: self._update_status(err_msg, True)); self.master.after(0, lambda: messagebox.showerror("Sync Error", f"An unexpected error occurred: {e}"))
        finally: self.master.after(0, self.update_button_states)

    def trigger_manual_gsheet_update(self):
        self._update_status("Manual Google Sheet update requested...")
        self.update_button_states(operation_running=True)
        threading.Thread(target=self._perform_gsheet_update_task, daemon=True).start()

    def _perform_gsheet_update_task(self):
        self.master.after(0, lambda: self._update_status("Manual GSheet update in progress..."))
        logger.info("Starting manual Google Sheet update task.")
        gsheet_update_ok = False; any_pets_for_sheet = False
        current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            all_pets_from_db = fetch_filtered_pets_from_db(self.config, archived_filter="All")
            any_pets_for_sheet = bool(all_pets_from_db)
            if any_pets_for_sheet:
                if self.config.getboolean('AppSettings', 'sync_archived_to_gsheet'):
                    pets_to_send_to_gsheet = all_pets_from_db
                else:
                    pets_to_send_to_gsheet = [p for p in all_pets_from_db if not p.get('archived')]

                logger.info("Calling update_google_sheet for manual GSheet update with %d pets...", len(pets_to_send_to_gsheet))
                if pets_to_send_to_gsheet:
                    gsheet_update_ok = update_google_sheet(pets_to_send_to_gsheet)
                else:
                    logger.info("No pets to send to GSheet after applying 'sync_archived_to_gsheet' setting for manual update.")
                    gsheet_update_ok = True
                logger.info(f"Manual update_google_sheet call completed. Success: {gsheet_update_ok}")
                if gsheet_update_ok:
                    self.master.after(0, lambda: self._update_status(f"Manual GSheet update complete at {datetime.now().strftime('%H:%M:%S')}."))
                    self._save_app_setting('last_successful_gsheet_update', current_time_str)
                    self.master.after(0, self._update_last_synced_labels)
                else: self.master.after(0, lambda: self._update_status("Manual GSheet update FAILED. Check logs.", True))
            else:
                logger.info("No pets in local DB to update GSheet with for manual update.")
                self.master.after(0, lambda: self._update_status("No data in local DB for GSheet update."))
                gsheet_update_ok = True 
        except Exception as e:
            logger.error(f"Error during manual GSheet update task: {e}", exc_info=True)
            self.master.after(0, lambda: self._update_status(f"Manual GSheet update FAILED: {e}", True))
            self.master.after(0, lambda: messagebox.showerror("GSheet Update Error", f"An error occurred: {e}"))
        finally: self.master.after(0, self.update_button_states)

    def refresh_pet_table(self):
        if not self.master.winfo_exists(): return
        search_term = self.search_term_var.get()
        species = self.species_filter_var.get()
        sex = self.sex_filter_var.get()
        archived_status = self.archived_filter_var.get()
        logger.info(f"Refreshing pet table. Filters - Search: '{search_term}', Species: '{species}', Sex: '{sex}', Status: '{archived_status}'")
        
        filtered_pets = fetch_filtered_pets_from_db(self.config, search_term, species, sex, archived_status)
        for i in self.pet_tree.get_children(): self.pet_tree.delete(i)
        if not filtered_pets:
            logger.info("No pets found matching current filter criteria.")
            self._update_status("No pets match current filters.")
            return
        try:
            for row_idx, row_data_dict in enumerate(filtered_pets):
                row_values = [row_data_dict.get(col, "") for col in self.COLUMNS]
                tag_name = 'archived' if row_data_dict.get('archived') == 1 else ('evenrow' if row_idx % 2 == 0 else 'oddrow')
                self.pet_tree.insert('', 'end', values=row_values, tags=(tag_name,))
            style = ttk.Style()
            style.configure("archived.Treeview", background="#FFDDDD", foreground="#555555") 
            self._update_photo_panel_display(None, "Select a pet to see photo")
            self._current_pet_photo = None
            self._update_status(f"Pet table refreshed. Displaying {len(filtered_pets)} pets.")
        except Exception as e:
            logger.error("Failed to populate pet table: %s", e, exc_info=True)
            self._update_status(f"Error refreshing table: {e}", True); messagebox.showerror("Table Error", f"Could not display data: {e}")

    def _on_pet_select(self, event: Optional[tk.Event]):
        selected_items = self.pet_tree.selection()
        if not selected_items: return
        item_values = self.pet_tree.item(selected_items[0])['values']
        if not item_values or len(item_values) < len(self.COLUMNS): self._update_photo_panel_display(None, "Error: Incomplete pet data."); return
        pet_data = dict(zip(self.COLUMNS, item_values))
        photo_url = pet_data.get('photo_url')
        if photo_url and photo_url != 'None' and photo_url.strip():
            self._update_photo_panel_display(None, "Loading image...")
            threading.Thread(target=self._load_image_threaded, args=(photo_url,), daemon=True).start()
        else: self._update_photo_panel_display(None, "No image available for this pet.")

    def _load_image_threaded(self, photo_url: str):
        try:
            response = requests.get(photo_url, timeout=10); response.raise_for_status()
            img = Image.open(BytesIO(response.content)); img.thumbnail((250, 250), Image.Resampling.LANCZOS)
            photo_image = ImageTk.PhotoImage(img)
            self.master.after(0, lambda: self._update_photo_panel_display(photo_image, None))
        except requests.exceptions.RequestException as e: logger.warning("Failed to load image from %s: %s", photo_url, e); self.master.after(0, lambda: self._update_photo_panel_display(None, f"Failed to load image:\n{e}"))
        except Exception as e: logger.error("Error processing image from %s: %s", photo_url, e, exc_info=True); self.master.after(0, lambda: self._update_photo_panel_display(None, f"Error displaying image:\n{e}"))

    def _update_photo_panel_display(self, image: Optional[ImageTk.PhotoImage], text_message: Optional[str]):
        if not self.master.winfo_exists(): return
        if image: self._current_pet_photo = image; self.photo_label.config(image=self._current_pet_photo, text='')
        else: self._current_pet_photo = None; self.photo_label.config(image='', text=text_message or "No image")

    def show_debug_log(self):
        log_win = tk.Toplevel(self.master); log_win.title("Application Debug Log"); log_win.geometry("800x600")
        txt_area = scrolledtext.ScrolledText(log_win, width=120, height=40, wrap=tk.WORD, state='disabled')
        txt_area.pack(padx=10, pady=10, fill='both', expand=True)
        def _refresh_log_content():
            txt_area.config(state='normal'); txt_area.delete('1.0', 'end')
            txt_area.insert('end', "\n".join(log_buffer)); txt_area.yview_moveto(1.0)
            txt_area.config(state='disabled')
        _refresh_log_content()
        btn_frame = ttk.Frame(log_win, padding=5); btn_frame.pack()
        ttk.Button(btn_frame, text="Refresh Log", command=_refresh_log_content).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Clear Log Buffer", command=lambda: (log_buffer.clear(), _refresh_log_content())).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Close", command=log_win.destroy).pack(side='left', padx=5)
        log_win.transient(self.master); log_win.grab_set()

    def _on_closing(self):
        if messagebox.askokcancel("Quit", f"Do you want to quit Petango Sync v{APP_VERSION}?"):
            logger.info("Application shutting down...")
            if self.sync_running and self.scheduler.state == STATE_RUNNING: 
                logger.info("Attempting to shut down scheduler...")
                try: 
                    self.scheduler.shutdown(wait=False) 
                    logger.info("Scheduler shut down.")
                except Exception as e: 
                    logger.error("Error shutting down scheduler: %s", e, exc_info=True)
            self.master.destroy()

# --- Main Application Execution ---
def launch_gui():
    app_config = load_config()
    initial_theme = app_config.get('AppSettings', 'theme', fallback=DEFAULT_SETTINGS['theme'])
    initial_log_level_str = app_config.get('AppSettings', 'log_level', fallback=DEFAULT_SETTINGS['log_level'])
    logger.setLevel(logging.DEBUG if initial_log_level_str == "DEBUG" else logging.INFO)
    logger.info(f"Initial log level set to: {logger.getEffectiveLevel()} ({initial_log_level_str}) from config.")
    root: Optional[ThemedTk | tk.Tk] = None
    try:
        try:
            logger.debug(f"Attempting to initialize ThemedTk with theme: {initial_theme}")
            root = ThemedTk(theme=initial_theme)
            if hasattr(root, 'set_theme'): 
                 root.set_theme(initial_theme)
            logger.info(f"Successfully applied initial theme: {root.current_theme if hasattr(root, 'current_theme') else initial_theme}")
        except tk.TclError as e:
            logger.warning(f"Failed to apply ThemedTk theme ('{initial_theme}'): {e}. Falling back to standard Tk with ttk theme attempt.")
            root = tk.Tk() 
            style = ttk.Style(root)
            available_ttk_themes = style.theme_names()
            final_theme_to_use = initial_theme
            if initial_theme not in available_ttk_themes: 
                final_theme_to_use = 'clam' if 'clam' in available_ttk_themes else (available_ttk_themes[0] if available_ttk_themes else 'default')
            try:
                if final_theme_to_use : style.theme_use(final_theme_to_use)
                logger.info(f"Applied fallback ttk theme: {final_theme_to_use}")
            except tk.TclError:
                 logger.warning(f"Could not apply any fallback ttk themes, using Tk default: {style.theme_use()}")
        init_db()
    except Exception as e:
        print(f"CRITICAL: Application startup failed: {e}") 
        logger.critical("Application startup failed: %s", e, exc_info=True)
        if root is None: 
            root_err_check = tk.Tk()
            root_err_check.withdraw()
            messagebox.showerror("Fatal Error", f"Application startup failed: {e}\nApplication will exit.")
            root_err_check.destroy()
        else: 
             messagebox.showerror("Fatal Error", f"Application startup failed: {e}\nApplication will exit.")
        return 
        
    if root is None: 
        logger.critical("Root window was not created. Exiting.")
        return 
        
    app = PetSyncGUI(root, app_config)
    root.mainloop()

if __name__ == '__main__':
    launch_gui()
