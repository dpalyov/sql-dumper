import PySimpleGUI as sg
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, URL, text, exc, Engine, select, table, column, func
import base64
from typing import Optional, Tuple, Union, Literal

load_dotenv()
exit_events = (sg.WIN_CLOSED, 'cancel', "Exit")


# Sets up connection to the database
def init_engine(user: str, passwd: str, db: str, host: str, driver: str) -> Optional[Engine]:

    if (db and host and driver) not in (None, ""):

        url_object = URL.create(username=user, password=passwd, host=host, drivername=driver,
                                database=db)
        engine = create_engine(url_object)
        return engine


# Initially gets all tables that are available on the configured database
def get_tables(engine: Engine, window: sg.Window, db: str):

    try:
        if engine is not None:
            with engine.begin() as conn:
                # db_tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + db + "'"

                info_tables = table("tables", column("table_name"), column(
                    "table_schema"), schema="information_schema")
                db_tables_query = select(info_tables.c.table_name).select_from(info_tables).where(
                    info_tables.c["table_schema"] == db)

                tables = conn.execute(db_tables_query)
                tables = tables.scalars().all()
                window.Element("tables").update(values=tables)
                window.Element("info").update("")
    except exc.DBAPIError:
        window.Element("info").update(
            "Error connecting to the database! Please check credentials or authenticate")


# This gets called when user needs to authenticate through GUI
def open_auth() -> Optional[Tuple[Union[Literal["username"], Literal["Password"]], str]]:
    layout = [
        [sg.Text("Username:"), sg.InputText(key="username")],
        [sg.Text("Password:"), sg.InputText(
            key="Password", password_char="*")],
        [sg.Submit(button_text="Login", bind_return_key=True,
                   auto_size_button=True)]
    ]

    window = sg.Window("Login", layout, modal=True, size=(
        450, 150), element_justification="center")

    result = None
    while True:
        event, values = window.read()

        if event in exit_events:
            break

        if event == "Login":
            result = (values["username"], values["Password"])
            break

    window.close()
    return result


def main():

    user = os.getenv("DB_USER")
    passwd = os.getenv("DB_PASSWORD")
    db = os.getenv("DB_SCHEMA")
    host = os.getenv("DB_HOST")
    driver = os.getenv("DB_DRIVER")
    theme = os.getenv("THEME") or "BrightColors"

    visible_login = True if (user or passwd) in (None, "") else False

    sg.theme(theme)

    # App Layout
    layout = [
        [sg.Text("Current theme:"), sg.Text(
            theme, text_color=sg.OLD_TABLE_TREE_SELECTED_ROW_COLORS[1]), sg.Text("User:"), sg.Text(user, key="username", text_color=sg.OLD_TABLE_TREE_SELECTED_ROW_COLORS[1])],
        [sg.Button(key="login", visible=visible_login, image_source="assets/login.png",
                   image_size=(50, 30))],
        [sg.Text("Available tables are:")],
        [sg.Listbox(key="tables", values=[],
                    size=(50, 5), enable_events=True, expand_x=True)],
        [sg.Text("Available columns are:")],
        [sg.Listbox(key="columns", values=[], size=(50, 5),
                    enable_events=True, select_mode=sg.SELECT_MODE_MULTIPLE, expand_x=True)],
        [sg.Text("What portion of the data to dump (in %)")],
        [sg.Slider(key="count", range=(0, 100), orientation="horizontal",
                   expand_x=True, enable_events=True)],
        [sg.Text("Enter SELECT SQL query here")],
        [sg.Multiline(size=(50, 5), key="textbox", expand_x=True)],
        [sg.Text("Filename"),
            sg.InputText(key="filename", default_text="myexport",
                         size=50, expand_x=True),
            sg.Text("Extension"),
            sg.DropDown(key="extension", default_value="txt", values=["txt", "json"])],
        [sg.Text(key="info", text_color=sg.DEFAULT_TEXT_COLOR)],
        [sg.Button(key="submit", image_source="assets/poop.png", tooltip="Dump!",
                   image_size=(60, 40)), sg.Button(key="cancel", tooltip="Cancel", image_source="assets/fart.png", image_size=(60, 40))]
    ]

    window_icon = None

    with open("assets/window_icon.png", "rb") as file:
        window_icon = base64.b64encode(file.read())

    window = sg.Window("SQL Dumper", layout, finalize=True,
                       resizable=True, icon=window_icon)

    engine = init_engine(user, passwd, db, host, driver)
    get_tables(engine, window, db)
    row_count = 0

    while True:
        event, values = window.read()

        # Closing the event loop
        if event in exit_events:
            break

        # Login is used only when no credentials are provided through environment
        if event == "login":
            auth_result = open_auth()
            user = auth_result["username"]
            passwd = auth_result["Password"]

            engine = init_engine(user, passwd, db, host, driver)
            get_tables(engine, window, db)
            window.Element("username").update(user)
            window.Element("submit").update(disabled=False)

        selected_table = values["tables"][0] if len(
            values["tables"]) > 0 else ""

        base_query = None
        extended_query = None
        percent = 0

        if selected_table:
            filter_columns = [column(c) for c in values["columns"]]
            base_query = select(
                *filter_columns or "*").select_from(table(selected_table))

            percent = int(values["count"])
            absolut = int(round(row_count * (percent / 100)))

            extended_query = str(base_query.limit(absolut)).replace(
                ":param_1", str(absolut))

        # Event handling when table selection change
        if event == "tables" and engine is not None:
            with engine.begin() as conn:
                info_columns = table("columns", column(
                    "column_name"), column("table_name"), schema="information_schema")

                table_columns_query = select(info_columns.c.column_name).select_from(
                    info_columns).where(info_columns.c.table_name == selected_table)

                count_query = select(func.count(
                    "*")).select_from(table(selected_table))

                columns = conn.execute(table_columns_query).scalars().all()
                row_count = conn.execute(count_query).scalar()

                window.Element("columns").update(values=columns)
                window.Element("textbox").update(
                    extended_query if percent > 0 else base_query)

        # Event handling when column selection change
        if event == "columns":
            window.Element("textbox").update(
                extended_query if percent > 0 else base_query)

        # Event handling when percent selection change
        if event == "count" and selected_table:
            window.Element("textbox").update(extended_query)

        # Event handling when submitting
        if event == "submit":

            query: str = values["textbox"]
            filename: str = values["filename"]
            ext = values["extension"]

            if filename == "":
                window.Element("info").update("Filename is required!")
                continue

            if query.upper().find("SELECT", ) < 0:
                window.Element("info").update(
                    "Query must be a SELECT statement!")
                continue

            df = pd.read_sql(query, engine)
            file = filename + "." + ext

            if ext == "txt":
                df.to_csv(file)

            if ext == "json":
                df.to_json(file, orient="table", indent=2)

            msg = 'Dumped %d records into file %s' % (len(df), file)
            print("Executed %s" % query)
            print(msg)

            window.Element("info").update(msg)

    window.close()


if __name__ == "__main__":
    main()
