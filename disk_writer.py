import os
import json
import joblib
import shutil
import pandas as pd
from datetime import datetime
from typing import Any, Dict, List, Tuple, Union


class DiskInteractions:
    """
    Methods for i/o-interacting with disk, including listing files and directories within a path,
    writing, reading, and deleting files, and creating, listing and deleting directories
    """

    def __init__(self) -> None:
        return

    @staticmethod
    def _set_upload_file_name(file_name: str) -> str:
        """
        Set the file name to upload by adding upload time to string
        :param file_name: the file_name

        :return: The file name with the time inserted into it
        """
        file_name_parts = os.path.splitext(file_name)
        return f"{file_name_parts[0]}_{datetime.utcnow().strftime('%Y.%m.%d_%H.%M.%S')}{file_name_parts[1]}"

    @staticmethod
    def _full_file_name(path: str = None, name: str = None) -> str:
        """
        Join a path and a name
        :param path: The path of the file
        :return: The combined name
        """
        return os.path.join(path, name)

    @staticmethod
    def get_current_directory() -> Union[str, None]:
        """
        Get the current directory path

        :return: The directory path
        """
        try:
            current_directory = os.getcwd()
            print(f"Current directory {current_directory} gathered")
            return current_directory
        except Exception as e:
            print(f"Getting current directory exception {e}")
            return None

    @staticmethod
    def create_directory(directory_path: str) -> bool:
        """
        Create a directory if it doesn't exist
        :param directory_path: path and directory name to create
        :return: bool of success or failure
        """
        try:
            if not os.path.exists(directory_path):
                os.mkdir(directory_path)
                print(f"Directory {directory_path} created")
                return True
            else:
                print(
                    f"Couldn't create directory {directory_path} as it already exists"
                )
                return False
        except Exception as e:
            print(f"Creating directory exception {e}")
            return False

    @staticmethod
    def create_multilevel_directories(directory_path: str) -> bool:
        """
        Create multi-level directories if they don't exist
        :param directory_path: path and multilevel directories' names to create
        :return: bool of success or failure
        """
        try:
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)
                print(f"Multi-level directories {directory_path} created")
                return True
            else:
                print(
                    f"Couldn't create multi-level directories {directory_path} as they already exist"
                )
        except Exception as e:
            print(f"Creating multilevel directories exception {e}")
            return False

    @staticmethod
    def list_files_in_directory(
        directory_path: str = "./", files_only: bool = False
    ) -> List[str]:
        """
        List the files (and directories when specified) in a directory
        :param directory_path: path to the directory
        :param files_only: flag to choose only files and exclude directories (True), or include them both (False)
        :return: list of files, and directories when applicable, under the directory
        """
        files = os.listdir(directory_path)
        if files_only:
            return [
                file
                for file in files
                if os.path.isfile(os.path.join(directory_path, file))
            ]
        return files

    @staticmethod
    def _ret_result_or_bool(
        ret_result: bool = True,
        result_when_true: bool = True,
        result_when_false: bool = True,
    ) -> bool:
        """
        Return the given boolean result or specify it with a flag value
        :param ret_result: flag to return the value from a result or to specify its value directly
        :param result_when_true: bool to return when the flag to return (ret_result) result is true
        :param result_when_false: bool to return when the flag to return (ret_result) result is false
        :return: flag with either the result value or the value specified
        """
        if ret_result:
            return result_when_true
        else:
            return result_when_false

    @staticmethod
    def _write_json_data(data_out: json, file_name: str) -> None:
        """
        Write a json object into a json file
        :param data_out: json to be written as a json file
        :param file_name: name of the json file to write into
        :return: None
        """
        with open(file_name, "w") as fp:
            json.dump(data_out, fp)

    @staticmethod
    def _write_csv_data(data_out: pd.DataFrame, file_name: str) -> None:
        """
        Write a df into a csv file
        :param data_out: df to be written as a csv file
        :param file_name: name of the csv file to write into
        :return: None
        """
        data_out.to_csv(file_name, index=False)

    @staticmethod
    def _write_obj_data(data_out: pd.DataFrame, file_name: str) -> None:
        """
        Write a python object by serializing it using joblib
        :param data_out: valid python object to be serialized
        :param file_name: name of the file to write the valid python object into
        :return: None
        """
        joblib.dump(data_out, file_name)

    def _write_fun_from_file_extension(self, file_name: str, data_out: Any) -> None:
        """
        Write an object into disk by calling the appropriate function to upload df into csv files, json-s into json
        files, and the rest of python objects serialized using joblib
        :param file_name: name of the file to write the object into
        :param data_out: data to be written in disk
        :return: None
        """
        if file_name.endswith(".json"):
            self._write_json_data(data_out=data_out, file_name=file_name)
        elif file_name.endswith(".csv"):
            self._write_csv_data(data_out=data_out, file_name=file_name)
        else:
            self._write_obj_data(data_out=data_out, file_name=file_name)

    def _write_file(self, file_name: str, data_out: Any) -> bool:
        """
        Try to write the data into a file in disk, and get the bool of success or failure
        :param file_name: name of the file to write the object into
        :param data_out: data to be written into disk
        :return: bool of success or failure
        """
        try:
            self._write_fun_from_file_extension(file_name=file_name, data_out=data_out)
            return True
        except Exception as e:
            print(f"Write exception {e}")
            return False

    def write_file_to_disk(
        self,
        data_out: Any,
        file_name: str,
        include_datetime: bool = False,
        ret_result: bool = False,
    ) -> bool:
        """
        Writes json, data-frame, or any valid python object to disk, and get the bool of success or failure

        :param data_out: data to write
        :param file_name: path and name of file to upload
        :param include_datetime: flag whether a datetime suffix must be appended to the file name (True) or not (False)
        :param ret_result: whether the result should be returned (True) or not (False)
        :return: bool of success or failure
        """
        if include_datetime:
            upload_file_name = self._set_upload_file_name(file_name)
        else:
            upload_file_name = file_name
        result = self._write_file(file_name=upload_file_name, data_out=data_out)
        if result:
            print(f"Upload to Disk Successful in {upload_file_name}")
            return self._ret_result_or_bool(
                ret_result=ret_result, result_when_true=result, result_when_false=True
            )
        else:
            print(f"Unsuccessful upload to Disk in {upload_file_name}")
            return self._ret_result_or_bool(
                ret_result=ret_result, result_when_true=result, result_when_false=False
            )

    def write_files_to_disk(
        self,
        file_name_to_data_out_list: List[Tuple[str, Any]],
        include_datetime: bool = False,
        ret_result: bool = False,
    ) -> Dict[str, bool]:
        """
        Writes multiple json, data-frame, or any valid python object to disk, and get the bool of success or failure
        for each of the attempted files to write

        :param file_name_to_data_out_list: list of tuples each containing the path-and-file-names and data to be written
        :param include_datetime: flag whether a datetime suffix must be appended to the file name (True) or not (False)
        :param ret_result: flag whether the success bool must be returned from the function or specified
        :return: dict mapping each file to the bool of success or failure
        """
        return_data = dict()
        for file_data in file_name_to_data_out_list:
            file_name = file_data[0]
            data_out = file_data[1]
            return_data[os.path.basename(file_name)] = self.write_file_to_disk(
                data_out=data_out,
                file_name=file_name,
                include_datetime=include_datetime,
                ret_result=ret_result,
            )
        return return_data

    @staticmethod
    def _read_json_data(file_name: str) -> json:
        """
        Read json data from disk

        :param file_name: path and name of the json file
        :return: json data
        """
        with open(file_name, "r") as fp:
            data_in = json.load(fp)
        return data_in

    @staticmethod
    def _read_csv_data(file_name: str, dtype_dict: Dict[str, str] = None) -> pd.DataFrame:
        """
        Read a csv file from disk

        :param file_name: path and name of the csv file
        :param dtype_dict: mapping of columns fields to their data types
        :return: pandas df with the data
        """
        return pd.read_csv(file_name, dtype=dtype_dict)

    @staticmethod
    def _read_obj_data(file_name: str) -> Any:
        """
        Read any valid python object from disk
        :param file_name: path and name of the python object file
        :return: python object data
        """
        return joblib.load(file_name)

    def _read_fun_from_file_extension(self, file_name: str, dtype_dict: Dict[str, str] = None) -> Any:
        """
        Read an object from disk by calling the appropriate function to read df from csv files, json-s from json
        files, and the rest of python objects from serialized objects using joblib

        :param file_name: name of the file to read the data from
        :param dtype_dict: mapping of columns fields to their data types
        :return: None
        """
        if file_name.endswith(".json"):
            return self._read_json_data(file_name=file_name)
        elif file_name.endswith(".csv"):
            return self._read_csv_data(file_name=file_name, dtype_dict=dtype_dict)
        else:
            return self._read_obj_data(file_name=file_name)

    def _read_file(self, file_name: str, dtype_dict: Dict[str, str] = None) -> Tuple[Any, bool]:
        """
        Try to read the data from a file in disk, and get the bool of success or failure
        :param file_name: name of the file to read the data from
        :param dtype_dict: mapping of columns fields to their data types
        :return: Tuple with the data when succeeded or None if not, and the bool of success or failure
        """
        try:
            return self._read_fun_from_file_extension(file_name=file_name, dtype_dict=dtype_dict), True
        except Exception as e:
            print(f"Read exception {e}")
            return None, False

    def read_file_from_disk(self, file_name: str, dtype_dict: Dict[str, str] = None) -> Any:
        """
        Read json, data-frame, or any valid python object from disk

        :param file_name: path and name of file to read from
        :param dtype_dict: mapping of columns fields to their data types
        :return: data read if successful, None otherwise
        """
        data_, result = self._read_file(file_name=file_name, dtype_dict=dtype_dict)
        if result:
            print(f"Read {file_name} from Disk Successful")
            return data_
        else:
            print(f"Unsuccessful read {file_name} from Disk")
            return data_

    def read_files_from_disk(self, files: List[Union[str, Tuple[str, Dict[str, str]]]]) -> Dict[str, Any]:
        """
        Read a list of files containing json, data-frame, or any valid python object from disk
        :param files: list of files
        :return: dict of the data, with file names as keys and the data as values
        """
        return_data = dict()
        for file in files:
            if isinstance(file, tuple):
                file_name = file[0]
                dtype_dict = file[1]
            else:
                file_name = file
                dtype_dict = None
            return_data[os.path.basename(file)] = self.read_file_from_disk(
                file_name=file_name, dtype_dict=dtype_dict
            )
        return return_data

    @staticmethod
    def _delete_file(file_name: str) -> bool:
        """
        Try to delete a file from disk, and get the bool of success or failure
        :param file_name: path and name of the file to delete
        :return: bool of success or failure
        """
        try:
            if os.path.exists(file_name):
                os.remove(file_name)
            return True
        except Exception as e:
            print(f"Delete exception {e}")
            return False

    def delete_file_from_disk(self, file_name: str, ret_result: bool = False) -> bool:
        """
        Delete a file from disk, and return the bool of success or failure
        :param file_name: path and name of the file
        :param ret_result:
        :return: bool of success or failure
        """
        result = self._delete_file(file_name=file_name)
        if result:
            print(f"Deletion of file {file_name} Successful")
            return self._ret_result_or_bool(
                ret_result=ret_result, result_when_true=result, result_when_false=True
            )
        else:
            print(f"Unsuccessful deletion of file {file_name}")
            return self._ret_result_or_bool(
                ret_result=ret_result, result_when_true=result, result_when_false=False
            )

    def delete_files_from_disk(
        self, files: List[str], ret_result: bool = False
    ) -> Dict[str, bool]:
        """
        Delete a list of files from disk
        :param files: list of path-and-file-names to be deleted
        :param ret_result: flag whether the success bool must be returned from the function or specified
        :return: dict mapping each file to the bool of success or failure
        """
        return_data = dict()
        for file in files:
            return_data[os.path.basename(file)] = self.delete_file_from_disk(
                file_name=file, ret_result=ret_result
            )
        return return_data

    def delete_empty_directory(self, directory_path: str) -> bool:
        """
        Delete an empty directory from disk if it exists

        :param directory_path: path to the empty directory to delete
        :return: bool of success or failure
        """
        try:
            if os.path.exists(directory_path):
                if not self.list_files_in_directory(
                    directory_path=directory_path, files_only=False
                ):
                    os.rmdir(directory_path)
                    print(f"Deletion of empty directory {directory_path} Successful")
                    return True
                else:
                    print(
                        f"Couldn't delete empty directory {directory_path} as it is not empty."
                    )
                    return False
            else:
                print(
                    f"Couldn't delete empty directory {directory_path} as it does not exist."
                )
                return False
        except OSError:
            print(f"Unsuccessful deletion of empty directory {directory_path}")
            return False

    @staticmethod
    def delete_directory(directory_path: str) -> bool:
        """
        Delete a directory from disk if it exists

        :param directory_path: path to the directory to delete
        :return: bool of success or failure
        """
        try:
            if os.path.exists(directory_path):
                shutil.rmtree(path=directory_path)
                print(f"Deletion of directory {directory_path} Successful")
                return True
            else:
                print(
                    f"Couldn't delete directory {directory_path} as it does not exist."
                )
                return False
        except OSError:
            print(f"Unsuccessful deletion of directory {directory_path}")
            return False


if __name__ == "__main__":
    test_write_csv = pd.DataFrame(
        data={
            "col_int": [1, 2, 3],
            "col_float": [1.0, 2.0, 3.0],
            "col_string": ["one", "two", "three"],
        }
    )
    test_write_json = {"key": "val", "other_key": "other_val"}
    test_write_obj = list([1, 2, 3, 4])

    disk_interactions = DiskInteractions()

    # 1. Get current directory
    root_path = disk_interactions.get_current_directory()
    print(root_path)

    # 2. Create directory
    directory_to_create = root_path + "/test_directory"
    print(os.path.exists(directory_to_create))
    disk_interactions.create_directory(directory_path=directory_to_create)
    print(os.path.exists(directory_to_create))

    # 3. Create multilevel directories
    multilevel_directory_1_to_create = root_path + "/test_multilevel_1"
    multilevel_directory_to_create = (
        multilevel_directory_1_to_create + "/test_multilevel_2"
    )
    print(os.path.exists(multilevel_directory_to_create))
    disk_interactions.create_multilevel_directories(
        directory_path=multilevel_directory_to_create
    )
    print(os.path.exists(multilevel_directory_to_create))

    # 4. List files
    print(
        disk_interactions.list_files_in_directory(
            directory_path=root_path, files_only=True
        )
    )
    print(
        disk_interactions.list_files_in_directory(
            directory_path=root_path, files_only=False
        )
    )

    # 5. Write files
    print(
        disk_interactions.list_files_in_directory(
            directory_path=directory_to_create, files_only=True
        )
    )
    disk_interactions.write_file_to_disk(
        data_out=test_write_csv, file_name=directory_to_create + "/test_write_1.csv"
    )
    disk_interactions.write_file_to_disk(
        data_out=test_write_json, file_name=directory_to_create + "/test_write_2.json"
    )
    disk_interactions.write_file_to_disk(
        data_out=test_write_obj, file_name=directory_to_create + "/test_write_3"
    )
    disk_interactions.write_file_to_disk(
        data_out="test_string", file_name=directory_to_create + "/test_write_4"
    )
    print(
        disk_interactions.list_files_in_directory(
            directory_path=directory_to_create, files_only=True
        )
    )

    # 6. Read files
    print(
        disk_interactions.read_file_from_disk(
            file_name=directory_to_create + "/test_write_1.csv"
        )
    )
    print(
        disk_interactions.read_file_from_disk(
            file_name=directory_to_create + "/test_write_2.json"
        )
    )
    print(
        disk_interactions.read_file_from_disk(
            file_name=directory_to_create + "/test_write_3"
        )
    )
    print(
        disk_interactions.read_file_from_disk(
            file_name=directory_to_create + "/test_write_4"
        )
    )
    file_list = ["test_write_1.csv", "test_write_2.json", "test_write_3"]
    file_list = [directory_to_create + f"/{i}" for i in file_list]
    print(disk_interactions.read_files_from_disk(files=file_list))

    # 7. Delete files
    print(
        disk_interactions.list_files_in_directory(
            directory_path=directory_to_create, files_only=True
        )
    )
    disk_interactions.delete_file_from_disk(
        file_name=directory_to_create + "/test_write_4"
    )
    print(
        disk_interactions.list_files_in_directory(
            directory_path=directory_to_create, files_only=True
        )
    )
    disk_interactions.delete_files_from_disk(files=file_list[0:2])
    print(
        disk_interactions.list_files_in_directory(
            directory_path=directory_to_create, files_only=True
        )
    )

    # 8. Delete directories
    print(os.path.exists(multilevel_directory_to_create))
    disk_interactions.delete_empty_directory(
        directory_path=multilevel_directory_to_create
    )
    print(os.path.exists(multilevel_directory_to_create))
    print(os.path.exists(multilevel_directory_1_to_create))
    disk_interactions.delete_directory(directory_path=multilevel_directory_1_to_create)
    print(os.path.exists(multilevel_directory_1_to_create))
    print(os.path.exists(directory_to_create))
    disk_interactions.delete_directory(directory_path=directory_to_create)
    print(os.path.exists(directory_to_create))
