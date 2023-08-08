import os
import logging
import uuid
import pathlib
from typing import Dict, Union, Any, List

class LWOrchestrator:
    """
     A very lightweight Orchestrator to run a series of modules.
    """

    def __init__(
        self,
        module_mapping: Dict[str, List[int]] = None,
        batch_id: str = "batch_1",
        segment_list: List[str] = None,
        run_config: Dict[int, Union[str, Dict[str, Any]]] = None,
        results_channel: str = "blob_storage",
        id_val: str = None,
    ):

        if segment_list is None:
            raise ValueError("A segment list must be added!")
        else:
            self.segment_list = segment_list

        self.id_val = id_val

        self.module_mapping = module_mapping

        self.module_config = None
        self.container = os.environ.get("STORAGE_CONTAINER")
        self.base_path = pathlib.Path(
            f"forecast/{batch_id}/segment_results/"
        )
        self.results_channel = "disk" if results_channel is None else results_channel
        self.batch_id = self.set_batch_id() if batch_id is None else batch_id

    @staticmethod
    def set_batch_id() -> str:
        """
        Get the batch_id of the process.
        :return: A string of the UUID
        """
        batch_id = str(uuid.uuid4())
        logging.info(f"Setting batch id to {batch_id}")
        return batch_id

    def _upload_file_to_output_channel(self, data_out: Any, file_name: str) -> None:
        """
        Apply the write function based on the output channel
        :param data_out: data to be uploaded to the channel
        :param file_name: name of the file to be uploaded
        :return: None
        """
        if self.results_channel == "disk":
            self.interactor.write_file_to_disk(data_out=data_out, file_name=file_name)
        elif self.results_channel == "azure_blob":
            key, name = os.path.split(file_name)
            self.interactor.write_file_to_blob_storage(
                data_out=data_out, key=key, file_name=name
            )
        else:
            logging.critical(
                f"Output channel {self.results_channel} is not valid ('disk', 'azure_blob')"
            )
            raise NotImplementedError()

    def write_output(self, data: Dict[str, Any], segment_list: List[str]) -> None:
        """
        Write the output data
        :param data: The data to write
        :param segment: the current segment
        :return: None
        """
        logging.info(f"Writing Orcha {segment_list} results")
        self._upload_file_to_output_channel(
            data,
            self.base_path.joinpath(f"{'_'.join(segment_list)}_results.json"),
        )
        logging.info(
            f"Output successfully written to {self.results_channel} for {self.batch_id}"
        )

    def select_segment(
        self, module_dict: Dict[int, Union[Dict[str, Any]]]
    ) -> Dict[int, Union[str, Dict[str, Any]]]:
        """
        Set the module config dict to contain only values in the segment
        :param module_dict: Module config dictionary
        :return: The run configuration dict with only the segment info
        """
        return {k: v for (k, v) in module_dict.items() if k in self.module_id_list}

    @staticmethod
    def retry_msg(messages) -> None:
        """
        A placeholder for retrying failed messages.
        :param messages: The failed messages
        :return: None
        """
        logging.info(f"failed messages: {messages}")

    def run_process(self, process_dict: Dict[str, Any]) -> Any:
        """
        Run a segment process
        :param process_dict: The dict describing the process, it's constructor args, the run function,
        and the run function args.
        Example:
            {
             "process": "run_queue",
             "constructor_options": {
                 "retailer": self.retailer,
                 "config": queue_process_config,
                 "batch_id": self.batch_id,
             },
             "run_command": "start_queue",
             "run_options": {},
            }
        :return: the return value of running the process
        """
        logging.info(f"Calling process: {process_dict['process']}")
        process_obj = self.module_mapping[process_dict["process"]](
            **process_dict["constructor_options"]
        )
        result = getattr(process_obj, process_dict["run_command"])(
            **process_dict["run_options"]
        )
        return {process_dict["process"]: result}

    @staticmethod
    def append_to_results(
        base_dict: Dict[str, Any], append_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Combine the results dictionaries
        :param base_dict: The base dict to add to
        :param append_dict: The dict to add
        :return: The combined dict
        """
        return {**base_dict, **append_dict}

    def run_segment(self) -> None:
        """
        Run the segment and write the results.
        :return: None
        """
        results = {}
        for process in self.module_id_list:
            result = self.run_process(self.run_config[process])
            if result:
                self.append_to_results(results, result)
        if results.get("run_queue", None) is not None:
            self.retry_msg(results["run_queue"])
        self.write_output(results, self.segment_list)


if __name__ == "__main__":
    # Example syntax
    LWOrchestrator(
        segment_list=["segment"],
        batch_id="test_1",
    ).run_segment()
