import logging
_log = logging.getLogger(__name__)

def sortRecordAttributes(attributes, processor, keep_all_attributes=True):
        processor_attributes = processor["attributes"]

        ## match record attribute to each processor attribute
        sorted_attributes = []
        processor_attributes_list = []
        for each in processor_attributes:
            attribute_name = each["name"]
            processor_attributes_list.append(attribute_name)
            attribute = next(
                (item for item in attributes if item["key"] == attribute_name), None
            )
            if attribute is None:
                _log.info(f"{attribute_name} is None")
            else:
                sorted_attributes.append(attribute)

        if keep_all_attributes:
            for attr in attributes:
                attribute_name = attr["key"]
                if attribute_name not in processor_attributes_list:
                    _log.debug(f"{attribute_name} was not in processor's attributes. adding this to the end of the sorted attributes list")
                    sorted_attributes.append(attr)

        return sorted_attributes