import PySimpleGUI as sg
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, select, table, column, func, inspect
import base64
import regex as re

load_dotenv()


def main():

    theme = os.getenv("THEME") or "BrightColors"
    sg.theme(theme)

    conn_str = os.getenv("DB_CONNECTION_STRING")
    user_pattern = re.compile(
        "^.+:\/\/(?<user>.+):.+\@(?<host>.+)\/(?<schema>.+)$")
    m = user_pattern.match(conn_str)

    # setup out dir
    user_home_dir = os.getenv("HOME")
    out_dir = os.getenv("OUT_DIR") or user_home_dir + "/.dump"
    out_dir_exists = os.path.isdir(out_dir)

    if out_dir_exists == False:
        try:
            os.mkdir(out_dir)
        except Exception as ex:
            print(ex)

    user = None
    host = None
    schema = None
    query_result = None
    window_icon = None
    row_count = 0

    if m is not None:
        user = m.group("user")
        host = m.group("host")
        schema = m.group("schema")

    theme_info = [sg.Text("Current theme:"), sg.Text(
        theme, text_color=sg.OLD_TABLE_TREE_SELECTED_ROW_COLORS[1])]

    connection_meta = [
        sg.Text("User:", visible=user is not None), sg.Text(user, visible=user is not None,
                                                            key="-USERNAME-", text_color=sg.OLD_TABLE_TREE_SELECTED_ROW_COLORS[1]),
        sg.Text("Host:", visible=host is not None), sg.Text(host, visible=host is not None,
                                                            key="-HOST-", text_color=sg.OLD_TABLE_TREE_SELECTED_ROW_COLORS[1]),
        sg.Text("Schema:", visible=schema is not None), sg.Text(schema, visible=schema is not None,
                                                                key="-SCHEMA-", text_color=sg.OLD_TABLE_TREE_SELECTED_ROW_COLORS[1])
    ]

    tables_meta = [
        [sg.Text("Available tables are:"), sg.Text("", key="-ROWS-META-")], [sg.Listbox(key="-TABLES-", values=[],
                                                                                        size=(50, 5), enable_events=True, expand_x=True)]
    ]

    columns_meta = [
        [sg.Text("Available columns are:")],
        [sg.Listbox(key="-COLUMNS-", values=[], size=(50, 5),
                    enable_events=True, select_mode=sg.SELECT_MODE_MULTIPLE, expand_x=True)],
    ]

    limit_control = [
        [sg.Text("What portion of the data to dump (in %)")],
        [sg.Slider(key="-COUNT-", range=(0, 100), orientation="horizontal",
                   expand_x=True, enable_events=True)],
    ]

    sql_control = [
        [sg.Text("Enter SELECT SQL query here")],
        [sg.Multiline(size=(50, 5), key="-TEXTBOX-", expand_x=True)],
    ]

    file_control = [
        [sg.Text("Filename"),
            sg.InputText(key="-FILENAME-",
                         default_text=out_dir + "/myexport", expand_x=True),
            sg.Text("Extension"),
            sg.DropDown(key="-EXT-", default_value="txt", values=["txt", "json", "xlsx"], expand_x=True)],
    ]

    app_info = sg.Text(key="-INFO-",
                       text_color=sg.DEFAULT_TEXT_COLOR)

    divider = [sg.HorizontalSeparator(pad=((0, 0), (20, 20)))]

    action_buttons = [
        [sg.Button(key="-SUBMIT-", image_source="assets/poop.png", tooltip="Dump!", image_size=(60, 40)),
         sg.Button(key="-PEEK-", tooltip="Only a fart! (Shows a sample of 5 rows)",
                   image_source="assets/fart.png", image_size=(60, 40)),
         app_info,
         sg.Push(),
         sg.Button(image_source="assets/cancel.png", image_size=(60, 40), key="-CANCEL-", bind_return_key=True)],
    ]

    query_sneak_peek = [sg.Frame(
        "Sneak Peek 5", key="-RESULT-CONTAINER-", layout=[], expand_x=True, expand_y=True)]

    # App Layout
    layout = [
        theme_info,
        connection_meta,
        tables_meta,
        columns_meta,
        limit_control,
        sql_control,
        file_control,
        divider,
        action_buttons,
        query_sneak_peek
    ]

    with open("assets/window_icon.png", "rb") as file:
        window_icon = base64.b64encode(file.read())

    window = sg.Window("SQL Dumper", layout, finalize=True,
                       resizable=True, icon=window_icon)

    window.bind("<Escape>", "-ESCAPE-")

    engine = create_engine(conn_str)

    with engine.begin():
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        window.Element("-TABLES-").update(values=tables)
        window.Element("-INFO-").update("")

    while True:
        event, values = window.read()

        # Closing the event loop
        if event in (sg.WIN_CLOSED, '-CANCEL-', "-ESCAPE-"):
            break

        selected_table = values["-TABLES-"][0] if len(
            values["-TABLES-"]) > 0 else ""

        base_query = None
        extended_query = None
        percent = 0

        if selected_table:
            filter_columns = [column(c) for c in values["-COLUMNS-"]]
            base_query = select(
                *filter_columns or "*").select_from(table(selected_table))

            percent = int(values["-COUNT-"])
            absolut = int(round(row_count * (percent / 100)))

            extended_query = str(base_query.limit(absolut)).replace(
                ":param_1", str(absolut))

        # Event handling when table selection change
        if event == "-TABLES-":
            with engine.begin() as conn:
                count_query = select(func.count(
                    "*")).select_from(table(selected_table))

                row_count = conn.execute(count_query).scalar()

                columns = [col["name"]
                           for col in inspect(engine).get_columns(selected_table)]

                window.Element("-COLUMNS-").update(values=columns)
                window.Element(
                    "-ROWS-META-").update("Total rows on selected table - " + str(row_count))
                window.Element("-TEXTBOX-").update(
                    extended_query if percent > 0 else base_query)

        # Event handling when column selection change
        if event == "-COLUMNS-":
            window.Element("-TEXTBOX-").update(
                extended_query if percent > 0 else base_query)

        # Event handling when percent selection change
        if event == "-COUNT-" and selected_table:
            window.Element("-TEXTBOX-").update(extended_query)

        if event == "-PEEK-":
            query: str = values["-TEXTBOX-"]
            limit_regex = ".+\s*?(?<limit>limit\s*?\d+)"
            columns_regex = "select\s*?(?<columns>.+)\s*?FROM"
            m = re.compile(limit_regex, flags=re.I | re.M).search(query)

            query = query.replace(m["limit"],
                                  "LIMIT 5") if m is not None else query + " LIMIT 5"

            with engine.begin() as conn:
                query_result = conn.execute(
                    text(query))

                m = re.compile(columns_regex, flags=re.I | re.M).search(query)

                unpack = (m["columns"] if m["columns"].find("*") < 0 else ", ".join(
                    columns)) + "\n"

                for t in query_result:
                    str_arr = map(lambda x: str(x), t)
                    unpack += ", ".join(str_arr) + "\n"

                window.extend_layout(window["-RESULT-CONTAINER-"], [[sg.Multiline(
                    unpack, key="-QUERY-RESULT-", size=(50, 5), expand_x=True, expand_y=True)]])

        # Event handling when submitting
        if event == "-SUBMIT-":

            query: str = values["-TEXTBOX-"]
            filename: str = values["-FILENAME-"]
            ext = values["-EXT-"]

            if filename == "":
                window.Element("-INFO-").update("Filename is required!")
                continue

            if query.upper().find("SELECT", ) < 0:
                window.Element("-INFO-").update(
                    "Query must be a SELECT statement!")
                continue

            df = pd.read_sql(query, engine)
            file = filename + "." + ext

            try:
                if ext == "txt":
                    df.to_csv(file)

                if ext == "json":
                    df.to_json(file, orient="table", indent=2)

                if ext == "xlsx":
                    df.to_excel(file)

                msg = 'Dumped %d records into file %s' % (len(df), file)
                print("Executed %s" % query)
                print(msg)
                window.Element("-INFO-").update(msg)
            except Exception as ex:
                print(ex)
                window.Element("-INFO-").update(ex)
                pass

    window.close()


if __name__ == "__main__":
    main()
