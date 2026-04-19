class DataReductionLibraryDesign:
    """Stub replacing the real DRLD parser from the METIS_Pipeline 'codes'
    package.  Provides an empty *dataitems* dict so that
    ``generate_raw_classes_from_drld()`` and
    ``generate_pro_classes_from_drld()`` simply do nothing.
    """

    def __init__(self):
        self.dataitems = {}
