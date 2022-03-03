from typing import List, NoReturn, Tuple

from pandas.core.frame import DataFrame
from app.model import ArchiveOperator
from app.aio import ArchiveExporter, GAReader
from app.archive_cell import ArchiveCell
from flask import request, jsonify
import pandas as pd
from app.archive_constants import (LABEL, SLASH,
                                   CELL_LIST_FILE_NAME, TEST_TYPE, FORMAT)
import uuid
import threading
# Routes
"tracker -> msg"
global status
status = {}
"tracker -> id"
global source
source = {}

# GA PUBLISH STATUS OPTIONS:
# STARTED, IN_PROGRESS, FINISHED


def root()->Tuple[str, int]:
    """
    Root route

    :return: Returns a tuple with a string and status code
    :rtype: Tuple[str, int]
    """
    return jsonify('Hello Battery Archive!')


def liveness()->Tuple[str, int]:
    """
    Liveness Route

    :return: Returns a tuple with a string and status code
    :rtype: Tuple[str, int]
    """
    return "Alive", 200


def readiness()->Tuple[str, int]:
    """
    Readiness Route

    :return: Returns a tuple with a string and status code 
    :rtype: Tuple[str, int]
    """
    return "Ready", 200


def finish(tracker):
    if tracker in status:
        status[tracker] = "FINISHED"
        return {"tracker": tracker, "dataset_id": source[tracker]}, 200
    return {"tracker": "not found", "dataset_id": source[tracker]}, 200


def ga_finish(tracker, cell, status):
    print("Working on", tracker)
    print("Cell", cell)
    print("STATUS", status)
    print("STATUS OF TRACKER", status[tracker])
    ArchiveOperator().add_cell_to_db(cell)
    status[tracker] = "FINISHED"


def ga_meta_finish(tracker, cell, status):
    print("Working on", tracker)
    print("Cell", cell)
    print("STATUS", status)
    print("STATUS OF TRACKER", status[tracker])
    ArchiveOperator().add_meta_to_db(cell)


def ga_data_finish(tracker, cell, status):
    print("Working on", tracker)
    print("Cell", cell)
    print("STATUS", status)
    print("STATUS OF TRACKER", status[tracker])
    ArchiveOperator().add_ts_to_db(cell)


def ga_publish(dataset_id):
    body = request.json
    token = body.get('token')
    tracker = str(uuid.uuid4())
    status[tracker] = "STARTED"
    source[tracker] = dataset_id
    gareader = GAReader(token)
    metadata, columns = gareader.read_metadata(int(dataset_id))
    if not metadata:
        return jsonify(
            {"message": "object is unreadable, missing a field or incorrect token", "dataset_id": dataset_id})
    cell_id = metadata[LABEL.CELL_ID.value]

    # Launch task into new thread
    status[tracker] = "IN_PROGRESS"
    data = gareader.read_data(int(dataset_id), columns)
    if not data:
        return jsonify(
            {"message": "object is unreadable, missing a field or incorrect token", "dataset_id": dataset_id})
    data[LABEL.CELL_ID.value] = cell_id
    data = pd.DataFrame(data=data, columns=data.keys())
    cell = ArchiveCell(cell_id,
                       test_type=TEST_TYPE.CYCLE.value,
                       metadata=metadata,
                       data=data)

    threading.Thread(target=ga_finish, name="data_thread",
                     args=(tracker, cell, status)).start()
    # # Add something from metadata into response
    return jsonify(
        {"tracker": tracker, "dataset_id": dataset_id, "token": token})


def ga_publish_status(tracker):
    if tracker in status and tracker in source:
        return jsonify(
            {"status": status[tracker], "dataset_id": source[tracker], "tracker": tracker})
    return jsonify({"status": "Unknown Tracker ID",
                    "dataset_id": "Unknown", "tracker": "Unknown"})


def get_cells():
    """get_cell
    Gets all cells
    :rtype: list of Cell
    :return: Return a tuple with a list of all the cell meta datas and status code
    :rtype: Tuple[List[dict], int]
    """
    ao = ArchiveOperator()
    archive_cells = ao.get_all_cell_meta()
    result = [cell.to_dict() for cell in archive_cells]
    return jsonify(result)


def get_cell_with_id(cell_id:str)->Tuple[List[dict], int]:
    """
    Get cell metadata using a Cell ID

    :param cell_id: String identifier for a cell
    :return: Return a tuple with a list of all cell metadata for selected cell and status code
    :rtype: Tuple[List[dict], int]
    """
    ao = ArchiveOperator()
    archive_cells = ao.get_all_cell_meta_with_id(cell_id)
    result = [cell.to_dict() for cell in archive_cells]
    return jsonify(result)


def get_test(test_name:str)->Tuple[List[dict], int]:
    """
    Get test results using a test name
    # TODO Catch invalide test names. 

    :param test_name: String identifier for a run test
    :return: Returns a tuple with a list of all requested test metadata and a status code 
    :rtype: Tuple[List[dict], int]
    """
    ao = ArchiveOperator()
    if test_name == TEST_TYPE.CYCLE.value:
        archive_cells = ao.get_all_cycle_meta()
        result = [cell.to_dict() for cell in archive_cells]
        return jsonify(result)
    if test_name == TEST_TYPE.ABUSE.value:
        archive_cells = ao.get_all_abuse_meta()
        result = [cell.to_dict() for cell in archive_cells]
        return jsonify(result)


def get_ts(test_name:str)->Tuple[List[dict], int]:
    """
    Get timeseries data using a test name 

    :param test_name: String identifier for a run test
    :return: Returns a tuple with a list of all requested timeseries data and a status code
    :rtype: Tuple[List[dict], int]
    """
    if test_name == TEST_TYPE.CYCLE.value:
        archive_cells = ArchiveOperator().get_all_cycle_ts()
        result = [cell.to_dict() for cell in archive_cells]
        return jsonify(result)
    if test_name == TEST_TYPE.ABUSE.value:
        archive_cells = ArchiveOperator().get_all_abuse_ts()
        result = [cell.to_dict() for cell in archive_cells]
        return jsonify(result)


def get_test_ts_with_id(cell_id:str, test_name:str)->Tuple[List[dict], int]:
    """
    Get timeseries data using a cell ID and a test name 

    :param cell_id: String identifier for a cell ID
    :type cell_id: str
    :param test_name: String identifier for a run test
    :type test_name: str
    :return: Returns a tuple with a list of all requested timeseries data for a single cell 
             and a status code
    :rtype: Tuple[List[dict], int]
    """
    if test_name == TEST_TYPE.CYCLE.value:
        archive_cells = ArchiveOperator().get_all_cycle_ts_with_id(cell_id)
        result = [cell.to_dict() for cell in archive_cells]
        return jsonify(result)
    if test_name == TEST_TYPE.ABUSE.value:
        archive_cells = ArchiveOperator().get_all_abuse_ts_with_id(cell_id)
        result = [cell.to_dict() for cell in archive_cells]
        return jsonify(result)


def get_meta_with_id(cell_id:str, test_name:str)->Tuple[List[dict], int]:
    """
    Get cycle metadata using a cell ID and test ID

    :param cell_id: [description]
    :type cell_id: str
    :param test_name: [description]
    :type test_name: str
    :return: Returns a tuple with a list of all requested cycle metadata for a single cell ID 
             and a status code
    :rtype: Tuple[List[dict], int]
    """
    if test_name == TEST_TYPE.CYCLE.value:
        archive_cells = ArchiveOperator().get_all_cycle_meta_with_id(cell_id)
        result = [cell.to_dict() for cell in archive_cells]
        return jsonify(result)
    if test_name == TEST_TYPE.ABUSE.value:
        archive_cells = ArchiveOperator().get_all_abuse_meta_with_id(cell_id)
        result = [cell.to_dict() for cell in archive_cells]
        return jsonify(result)


def add_cell()->Tuple[str, int]:
    """
    Post type request to add a cell to the database. Expects a path inside the receieved JSON
    to an uploaded excel spreadsheet. 

    :return: Returns a tuple with either successful or failed upload and a status code
    :rtype: Tuple[str, int]
    """
    body = request.json
    path = body.get('path')
    print(path)
    if import_cells_xls_to_db(path):
        return "Upload Successful", 200
    return "Upload Failed", 200


# EXPORTERS


def export_cycle_cells_to_fmt(cell_list_path: str,
                              output_path: str,
                              fmt: str = "csv")->NoReturn:
    """
    Export cycle cells to a format

    #TODO A some sort of return value. Functions invoked return a status boolean

    :param cell_list_path: A string path to an xlsx file.
    :type cell_list_path: str
    :param output_path: A string path to the generated output file
    :type output_path: str
    :param fmt: Format for output file, defaults to "csv"
    :type fmt: str, optional
    :return: This function doesn't return. Instead it calls a format specific function
    :rtype: NoReturn
    """
    #TODO: This implies cell_list must be xlsx, this can be written in CSV
    df_excel = pd.read_excel(cell_list_path + CELL_LIST_FILE_NAME)
    # TODO: Refactor this to a join instead of looping slowly
    for i in df_excel.index:
        cell_id = df_excel[LABEL.CELL_ID.value][i]
        df = ArchiveOperator().get_df_cell_meta_with_id(cell_id)
        if not df.empty:
            if fmt == FORMAT.CSV.value:
                export_cycle_meta_data_with_id_to_fmt(cell_id, output_path,
                                                      FORMAT.CSV.value)
                export_cycle_ts_data_csv(cell_id, output_path)
            if fmt == FORMAT.FEATHER.value:
                export_cycle_meta_data_with_id_to_fmt(cell_id, output_path,
                                                      FORMAT.FEATHER.value)
                export_cycle_ts_data_feather(cell_id, output_path)


def export_cycle_meta_data_with_id_to_fmt(cell_id: str,
                                          out_path: str,
                                          fmt: str = "csv")->str:
    """
    Export the cycle metadata for the selected cell ID

    :param cell_id: String identifier for a cell
    :type cell_id: str
    :param out_path: String output path for generated file
    :type out_path: str
    :param fmt: Format of generated file, defaults to "csv"
    :type fmt: str, optional
    :return: Returns the string of the generated file
    :rtype: str
    """
    if fmt == FORMAT.CSV.value:
        return ArchiveExporter.write_to_csv(
            ArchiveOperator().get_df_cycle_meta_with_id(cell_id), cell_id,
            out_path, "cycle_data")
    if fmt == FORMAT.FEATHER.value:
        return ArchiveExporter.write_to_feather(
            ArchiveOperator().get_df_cycle_meta_with_id(cell_id), cell_id,
            out_path, "cycle_data")

def export_cycle_ts_data_csv(cell_id: str, path: str)->str:
    """
    Export cycle timeseries data to a CSV file

    :param cell_id: String identifier for a cell
    :type cell_id: str
    :param path: String output path for generated file
    :type path: str
    :return: Returns the string of the generated file
    :rtype: str
    """
    return ArchiveExporter.write_to_csv(
        ArchiveOperator().get_df_cycle_ts_with_cell_id(cell_id), cell_id, path,
        "timeseries_data")

def export_cycle_ts_data_feather(cell_id: str, path: str)->str:
    """
    Export cycle timeseries data to a Feather file

    :param cell_id: String identifier for a cell
    :type cell_id: str
    :param path: String output path for generated file
    :type path: str
    :return: Returns the string of the generated file
    :rtype: str
    """
    return ArchiveExporter.write_to_feather(
        ArchiveOperator().get_df_cycle_ts_with_cell_id(cell_id), cell_id, path,
        "timeseries_data")


# Importers


def import_cells_xls_to_db(cell_list_path:str)->bool:
    """
    Import cells from an Excel spreadsheet to the database 

    :param cell_list_path: String path for cell list excel file
    :type cell_list_path: str
    :return: Return True if cells were added to the DB. Otherwise False
    :rtype: bool
    """
    return add_df_to_db(pd.read_excel(cell_list_path + CELL_LIST_FILE_NAME),
                        cell_list_path)


def import_cells_feather_to_db(cell_list_path:List[str])->bool:
    """
    Import cells from an Excel spreadsheet to the database 

    :param cell_list_path: String path for cell list excel file
    :type cell_list_path: str
    :return: Return True if cells were added to the DB. Otherwise False
    :rtype: bool
    """
    return add_df_to_db(pd.read_feather(cell_list_path + CELL_LIST_FILE_NAME),
                        cell_list_path)


def add_df_to_db(df:DataFrame, cell_list_path:str)->bool:
    """
    Add a dataframe of cells to the database

    :param df: DataFrame of cells imported 
    :type df: DataFrame
    :param cell_list_path: String path from where cells were imported
    :type cell_list_path: str
    :return: Return True if cells were added to the DB. Otherwise False
    :rtype: bool
    """
    cells = []
    for i in df.index:
        cell = ArchiveCell(cell_id=df[LABEL.CELL_ID.value][i],
                           test_type=str(df[LABEL.TEST.value][i]),
                           file_id=df[LABEL.FILE_ID.value][i],
                           file_type=str(df[LABEL.FILE_TYPE.value][i]),
                           tester=df[LABEL.TESTER.value][i],
                           file_path=cell_list_path +
                           df[LABEL.FILE_ID.value][i] + SLASH,
                           metadata=df.iloc[i])
        cells.append(cell)
    return ArchiveOperator().add_cells_to_db(cells)


def update_cycle_cells(cell_list_path:str)->bool:
    """
    Update cells with new data

    :param cell_list_path: Path to file containing cell data
    :type cell_list_path: str
    :return: Returns true
    :rtype: bool
    """
    df_excel = pd.read_excel(cell_list_path + CELL_LIST_FILE_NAME)
    ao = ArchiveOperator()
    for i in df_excel.index:
        cell_id = df_excel[LABEL.CELL_ID.value][i]
        df = ArchiveOperator().get_df_cell_meta_with_id(cell_id)
        if df.empty:
            print("cell:" + cell_id + " not found")
            continue
        ao.remove_cell_from_archive(cell_id)
    import_cells_xls_to_db(cell_list_path)
    return True
