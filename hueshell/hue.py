import argparse
import json
import os
import sys
import time
import uuid
import urllib.parse
from pathlib import Path
from configparser import ConfigParser

import requests

from hueshell import module_path


class Hue:
    LOGIN_PATH = "/accounts/login/"
    CREATE_NOTEBOOK_PATH = "/notebook/api/create_notebook"
    CREATE_SESSION_PATH = "/notebook/api/create_session"
    CLOSE_STATEMENT_PATH = "/notebook/api/close_statement"
    EXECUTE_PATH = "/notebook/api/execute/{query_type}"
    CHECK_STATUS_PATH = "/notebook/api/check_status"
    FETCH_RESULT_PATH = "/notebook/api/fetch_result_data"

    def __init__(self, url, username, password, engine="hive"):
        self.url = url
        self.username = username
        self.password = password
        self.engine = engine

        self.http_session = requests.Session()
        self.snippet_id = self.get_uuid()
        self.result_id = self.get_uuid()

    @classmethod
    def get_uuid(cls):
        return str(uuid.uuid4())

    def get_full_url(self, path):
        return urllib.parse.urljoin(self.url, path)

    def do_post(self, full_url, form_data={}):
        form_data.update({
            "csrfmiddlewaretoken": self.http_session.cookies["csrftoken"],
        })
        return self.http_session.post(full_url, data=form_data)

    def do_get(self, full_url):
        return self.http_session.get(full_url)

    def login(self):
        login_url = self.get_full_url(self.LOGIN_PATH)
        self.do_get(login_url)

        form_data = {
            "username": self.username,
            "password": self.password,
            "fromModal": "true",
        }
        response = self.do_post(login_url, form_data)
        response_dict = response.json()
        return response_dict["auth"]

    def create_notebook(self, directory_uuid=""):
        create_notebook_url = self.get_full_url(self.CREATE_NOTEBOOK_PATH)
        form_data = {
            "type": self.engine,
            "directory_uuid": directory_uuid,
        }
        response = self.do_post(create_notebook_url, form_data=form_data)
        response_dict = response.json()
        if response_dict["status"] == 0:
            return response_dict["notebook"]
        else:
            return None

    def create_session(self, notebook):
        create_session_url = self.get_full_url(self.CREATE_SESSION_PATH)
        notebook_dict = {
            "id": None,
            "uuid": notebook["uuid"],
            "parentSavedQueryUuid": None,
            "isSaved": notebook["isSaved"],
            "sessions": notebook["sessions"],
            "type": notebook["type"],
            "name": notebook["name"],
        }
        session_dict = {
            "type": self.engine,
        }
        notebook_json = json.dumps(notebook_dict)
        session_json = json.dumps(session_dict)

        form_data = {
            "notebook": notebook_json,
            "session": session_json,
        }
        response = self.do_post(create_session_url, form_data)
        response_dict = response.json()
        if response_dict["status"] == 0:
            return response_dict["session"]
        else:
            return None

    def close_statement(self, notebook, session, sql):
        close_statement_url = self.get_full_url(self.CLOSE_STATEMENT_PATH)
        notebook_dict = {
            "id": None,
            "uuid": notebook["uuid"],
            "parentSavedQueryUuid": None,
            "isSaved": False,
            "sessions": [session],
            "type": notebook["type"],
            "name": notebook["name"],
        }

        snippet_dict = {
            "id": self.snippet_id,
            "type": session["type"],
            "status": "ready",
            "statementType": "text",
            "statement": sql,
            "aceCursorPosition": {"row": 0, "column": 15},
            "statementPath": "",
            "associatedDocumentUuid": None,
            "properties": {"settings": [], "files": [], "functions": [], "arguments": []},
            "result": {
                "id": self.result_id,
                "type": "table",
                "handle": {},
            },
            "database": "default",
            "wasBatchExecuted": False,
        }

        notebook_json = json.dumps(notebook_dict)
        snippet_json = json.dumps(snippet_dict)

        form_data = {
            "notebook": notebook_json,
            "snippet": snippet_json,
        }
        response = self.do_post(close_statement_url, form_data)
        return response.json()

    def execute(self, notebook, session, sql):
        real_path = self.EXECUTE_PATH.format(query_type=session["type"])
        execute_real_url = self.get_full_url(real_path)

        snippet_dict = {
            "id": self.snippet_id,
            "type": session["type"],
            "status": "running",
            "statementType": "text",
            "statement": sql,
            "statement_raw": sql,
            "aceCursorPosition": {"row": 0, "column": 15},
            "statementPath": "",
            "associatedDocumentUuid": None,
            "properties": {"settings": [], "files": [], "functions": [], "arguments": []},
            "result": {
                "id": self.result_id,
                "type": "table",
                "handle": {},
            },
            "database": "default",
            "wasBatchExecuted": False,
        }

        notebook_dict = {
            "id": None,
            "uuid": notebook["uuid"],
            "parentSavedQueryUuid": None,
            "isSaved": False,
            "sessions": [session],
            "type": notebook["type"],
            "name": notebook["name"],
            "snippets": [snippet_dict],
        }

        notebook_json = json.dumps(notebook_dict)
        snippet_json = json.dumps(snippet_dict)
        form_data = {
            "notebook": notebook_json,
            "snippet": snippet_json,
        }

        response = self.do_post(execute_real_url, form_data)
        return response.json()

    def wait_and_return_result(self, notebook, session, sql, execute_return):
        check_status_url = self.get_full_url(self.CHECK_STATUS_PATH)
        fetch_result_url = self.get_full_url(self.FETCH_RESULT_PATH)
        snippet_dict = {
            "id": self.snippet_id,
            "type": session["type"],
            "status": "running",
            "statementType": "text",
            "statement": sql,
            "statement_raw": sql,
            "aceCursorPosition": {"row": 0, "column": 15},
            "statementPath": "",
            "associatedDocumentUuid": None,
            "properties": {"settings": [], "files": [], "functions": [], "arguments": []},
            "result": {
                "id": self.result_id,
                "type": "table",
                "handle": execute_return["handle"],
            },
            "database": "default",
            "wasBatchExecuted": False,
        }

        notebook_dict = {
            "id": execute_return["history_id"],
            "uuid": notebook["uuid"],
            "parentSavedQueryUuid": None,
            "isSaved": False,
            "sessions": [session],
            "type": notebook["type"],
            "name": notebook["name"],
            "snippets": [snippet_dict],
        }

        notebook_json = json.dumps(notebook_dict)
        snippet_json = json.dumps(snippet_dict)
        form_data = {
            "notebook": notebook_json,
            "snippet": snippet_json,
        }

        status_response = self.do_post(check_status_url, form_data)
        status_response_dict = status_response.json()
        while status_response_dict["status"] == 0 and status_response_dict["query_status"]["status"] == "running":
            time.sleep(1)
            status_response = self.do_post(check_status_url, form_data)
            status_response_dict = status_response.json()
            sys.stderr.write(f"{json.dumps(status_response_dict)}\n")

        start_over = "true"
        rows = 100
        form_data.update({
            "rows": rows,
            "startOver": start_over,
        })
        fetch_result_response = self.do_post(fetch_result_url, form_data)
        fetch_result_dict = fetch_result_response.json()
        start_over = "false"

        meta = [item["name"] for item in fetch_result_dict["result"]["meta"]]
        for item in fetch_result_dict["result"]["data"]:
            yield json.dumps(dict(zip(meta, item)))

        while fetch_result_dict["status"] == 0 and \
                fetch_result_dict["result"]["has_more"]:
            form_data.update({
                "rows": rows,
                "startOver": start_over
            })
            fetch_result_response = self.do_post(fetch_result_url, form_data)
            fetch_result_dict = fetch_result_response.json()
            meta = [item["name"] for item in fetch_result_dict["result"]["meta"]]
            for item in fetch_result_dict["result"]["data"]:
                yield json.dumps(dict(zip(meta, item)))


def execute_from_commandline(argv=None):
    conf_filename = ".hue.ini"
    home = Path.home().as_posix()
    conf_home = os.path.join(home, conf_filename)
    conf_module = os.path.join(module_path, conf_filename)
    conf_parser = ConfigParser()
    if os.path.exists(conf_home):
        conf_parser.read(conf_home)
    elif os.path.exists(conf_module):
        conf_parser.read(conf_module)
    else:
        pass

    hue_url = conf_parser.get("hue", "url")
    hue_username = conf_parser.get("hue", "username")
    hue_password = conf_parser.get("hue", "password")
    hue_default_engine = conf_parser.get("hue", "default_engine", fallback="hive")

    arg_parser = argparse.ArgumentParser(
        description="Hue Shell Program"
    )

    arg_parser.add_argument("--engine", action="store", required=False, dest="engine", default=hue_default_engine)
    arg_parser.add_argument("--sql", action="store", dest="sql", required=True)
    parsed_args = arg_parser.parse_args()
    hue = Hue(hue_url, hue_username, hue_password, engine=parsed_args.engine)
    login_result = hue.login()
    if not login_result:
        sys.stderr.write("login failed\n")
        raise RuntimeError("Login failed")

    notebook_result = hue.create_notebook()
    session_result = hue.create_session(notebook_result)
    if hue.engine == "hive":
        close_statement_result = hue.close_statement(notebook_result, session_result, parsed_args.sql)
    execute_result = hue.execute(notebook_result, session_result, parsed_args.sql)
    result = hue.wait_and_return_result(notebook_result, session_result, parsed_args.sql, execute_result)
    for item in result:
        sys.stdout.write(f"{item}\n")




