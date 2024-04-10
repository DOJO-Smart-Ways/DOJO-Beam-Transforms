# data_understanding.py
import apache_beam as beam

class ComparePCollectionsDetailed(beam.PTransform):
    """
    A custom PTransform for comparing two PCollections in detail.
    It identifies unique elements in each PCollection based on a provided key extractor function.
    """
    def __init__(self, pcol1, pcol2, key_extractor):
        """
        Initializes the ComparePCollectionsDetailed instance.
        
        Args:
            pcol1 (PCollection): The first PCollection to compare.
            pcol2 (PCollection): The second PCollection to compare.
            key_extractor (function): A function to extract keys from the elements of the PCollections.
        """
        self.pcol1 = pcol1
        self.pcol2 = pcol2
        self.key_extractor = key_extractor

    def expand(self, pcolls):
        """
        The core logic for comparing the PCollections.
        
        Args:
            pcolls: The input PCollection (not used, as inputs are provided during initialization).
            
        Returns:
            A PCollection containing messages about unique elements and counts of total elements.
        """
        # Apply the key extractor to both PCollections
        keyed_pcol1 = self.pcol1 | 'Key PCollection1' >> beam.Map(lambda x: (self.key_extractor(x), x))
        keyed_pcol2 = self.pcol2 | 'Key PCollection2' >> beam.Map(lambda x: (self.key_extractor(x), x))

        # Perform a CoGroupByKey
        grouped = (
            {'pcol1': keyed_pcol1, 'pcol2': keyed_pcol2}
            | 'CoGroupByKey' >> beam.CoGroupByKey()
        )

        # Identify elements unique to each PCollection
        def find_unique_elements(element):
            key, grouped_dict = element
            pcol1_elements = {str(e) for e in grouped_dict['pcol1']}
            pcol2_elements = {str(e) for e in grouped_dict['pcol2']}

            unique_to_pcol1 = pcol1_elements - pcol2_elements
            unique_to_pcol2 = pcol2_elements - pcol1_elements

            if unique_to_pcol1:
                yield f'Unique in PCollection1: {unique_to_pcol1}'
            if unique_to_pcol2:
                yield f'Unique in PCollection2: {unique_to_pcol2}'

        # Count total elements in each PCollection for verification
        total_elements_pcol1 = self.pcol1 | 'Count Total PCollection1' >> beam.combiners.Count.Globally()
        total_elements_pcol2 = self.pcol2 | 'Count Total PCollection2' >> beam.combiners.Count.Globally()

        # Combine all results for output
        return ((
            grouped | 'Find Different Elements' >> beam.FlatMap(find_unique_elements),
            total_elements_pcol1,
            total_elements_pcol2 )
        | 'Flatten All Results' >> beam.Flatten()
        )


class ExtractUniqueValuesFn(beam.DoFn):
    """
    A DoFn for extracting unique values from a specified field across elements in a PCollection.
    
    This function tracks seen values in a set to ensure each output is unique within the scope
    of this DoFn instance. Note that uniqueness is maintained within the context of a single DoFn instance,
    which means in a distributed environment, each instance may produce unique values, but duplicates
    may still exist across different instances. For true global uniqueness, consider using a more
    advanced pattern like combining all values and filtering globally.
    
    Attributes:
        key (str): The key of the dictionary element to extract for uniqueness.
        seen (set): A set to keep track of values that have already been processed and yielded.
    """

    def __init__(self, key):
        """
        Initializes the ExtractUniqueValuesFn with a specific field key to check for uniqueness.

        Args:
            key (str): The key of the dictionary element to extract for uniqueness.
        """
        self.key = key
        self.seen = set()  # Initialize the set to track seen values

    def process(self, element):
        """
        Processes each element in the PCollection, yielding unique values based on the specified key.

        Args:
            element (dict): An element of the PCollection, expected to be a dictionary.
            
        Yields:
            The value of the specified key from the element, if it hasn't been seen before.
        """
        # Access the value using the specified key
        value = element.get(self.key)
        # Check if the value has not been seen before
        if value not in self.seen:
            self.seen.add(value)  # Mark the value as seen
            yield value  # Yield the unique value


class ExtractColumnValuesFn(beam.DoFn):
    def __init__(self, column_name):
        self.column_name = column_name

    def process(self, element):
        # Yield the value if the column_name key exists; otherwise, yield a special value
        yield element.get(self.column_name, None)

def extract_and_get_distinct_values(p_collection, column_name):
    """
    Extracts values from a specified column in a PCollection and finds distinct values.

    Args:
        p_collection: The input PCollection.
        column_name: The name of the column to extract values from.

    Returns:
        A PCollection containing distinct values from the specified column.

    E.g.
        distinct_cost_centre = extract_and_get_distinct_values(pipeline, business_employee, 'COST_CENTRE')
        distinct_cost_centre | beam.Map(print)
    """
    # Define the Beam pipeline steps for extracting distinct values
    distinct_values = (
        p_collection
        | f"ExtractColumnValues: {column_name}" >> beam.ParDo(ExtractColumnValuesFn(column_name))
        | f"FilterNoneValues: {column_name}" >> beam.Filter(lambda x: x is not None)
        | f"DistinctValues: {column_name}" >> beam.Distinct()
    )
    return distinct_values
