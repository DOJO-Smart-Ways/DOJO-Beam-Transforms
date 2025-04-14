from enum import Enum
from typing import List, Tuple, Dict, Any
from apache_beam import PCollection
import apache_beam as beam
import logging

class Join(beam.PTransform):
    """
    A PTransform that performs a left join between two PCollections based on specified keys.

    Attributes:
        join_table (PCollection): The right table to join.
        keys (List[Tuple[str, str]]): A list of tuples specifying the join keys for the left and right tables.
    """
    def __init__(self, join_table: PCollection, keys: List[Tuple[str, str]], join_type: str) -> beam.PCollection:
        """
        Initializes the LeftJoin PTransform.

        Args:
            join_table (PCollection): The right table to join.
            keys (List[Tuple[str, str]]): A list of tuples specifying the join keys for the left and right tables.

        """
        supported_join_types = ['left', 'inner', 'right']
        if join_type not in supported_join_types:
            raise ValueError(f"Unsupported join_type '{join_type}', supported types are: {supported_join_types}")
        if not keys or not all(isinstance(key, tuple) and len(key) == 2 for key in keys):
            raise ValueError("Keys must be a non-empty list of tuples with two elements each.")
        self.join_table: PCollection = join_table
        self.keys: List[Tuple[str, str]] = keys
        self.join_type: str = join_type


    def expand(self, left_pcoll: PCollection) -> PCollection:
        """
        Expands the PTransform to perform the left join.

        Args:
            left_pcoll (PCollection): The left table (PCollection) to join.

        Returns:
            PCollection: The resulting PCollection after the left join.
        """
        # Extract all keys from the appropriate table based on join type
        if self.join_type == 'left' or self.join_type == 'inner':
            all_keys = (
                self.join_table
                | 'Extract Right Keys' >> beam.FlatMap(lambda x: x.keys())
                | 'Get Unique Right Keys' >> beam.Distinct()
                | 'Convert to Set' >> beam.combiners.ToSet()
            )
        elif self.join_type == 'right':
            all_keys = (
                left_pcoll
                | 'Extract Left Keys' >> beam.FlatMap(lambda x: x.keys())
                | 'Get Unique Left Keys' >> beam.Distinct()
                | 'Convert to Set Left' >> beam.combiners.ToSet()
            )
        

        # Create key-value pairs for the left table
        left_kv = left_pcoll | 'Key Left Table' >> beam.Map(self._create_key_value, self.keys, side='left')

        # Create key-value pairs for the right table
        right_kv = self.join_table | 'Key Right Table' >> beam.Map(self._create_key_value, self.keys, side='right')

        # Perform CoGroupByKey to group values by keys
        joined = (
            {'left': left_kv, 'right': right_kv}
            | 'CoGroupByKey' >> beam.CoGroupByKey()
            | 'Process Joined Results' >> beam.ParDo(self._process_joined_results(), beam.pvalue.AsSingleton(all_keys))
        )

        return joined

    @staticmethod
    def _create_key_value(element: Dict[str, Any], keys: List[Tuple[str, str]], side: str) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
        """
        Creates a key-value pair for the given element based on the specified keys.

        Args:
            element (Dict[str, Any]): The input element.
            keys (List[Tuple[str, str]]): The list of key mappings for the join.
            side (str): Indicates whether the element is from the 'left' or 'right' table.

        Returns:
            Tuple[Tuple[Any, ...], Dict[str, Any]]: A key-value pair where the key is a tuple of the join keys and the value is the element.
        """
        key = tuple(element[key[0 if side == 'left' else 1]] for key in keys)
        return key, element

    def _process_joined_results(self) -> beam.DoFn:
        print(self.join_type)
        """
        Processes the results of the CoGroupByKey to perform the left join.

        Returns:
            beam.DoFn: A DoFn to process the joined results.
        """
        class ProcessJoinedResultsFn(beam.DoFn):
            def __init__(self, join_type):
                self.join_type = join_type

            def process(self, element: Tuple[Tuple[Any, ...], Dict[str, List[Dict[str, Any]]]], all_keys: set):
                def rename_conflicting_columns(left_value, right_value):
                    """
                    Renames columns in the right table if they conflict with columns in the left table.
                    """
                    conflicting_keys = set(left_value.keys()) & set(right_value.keys())
                    renamed_right_value = {
                        (f"{k}_2" if k in conflicting_keys else k): v
                        for k, v in right_value.items()
                    }
                    return renamed_right_value
                
                key, grouped = element
                left_values = (
                    grouped['left'] or [{key: None for key in all_keys}]
                    if self.join_type == 'right'
                    else grouped['left']
                ) 
                right_values = (
                    grouped['right'] or [{key: None for key in all_keys}]
                    if self.join_type == 'left'
                    else grouped['right']
                )

                if self.join_type == 'left':
                    for left_value in left_values:
                        for right_value in right_values:
                            yield {**left_value, **rename_conflicting_columns(left_value, right_value)}
                
                elif self.join_type == 'inner':
                    if grouped['right']:
                        for left_value in left_values:
                            for right_value in right_values:
                                yield {**left_value, **rename_conflicting_columns(left_value, right_value)}
                
                elif self.join_type == 'right':
                    for right_value in right_values:
                        for left_value in left_values:
                            yield {**rename_conflicting_columns(left_value, right_value), **left_value}
                

        return ProcessJoinedResultsFn(self.join_type)