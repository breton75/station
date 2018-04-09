import defs


FIELD_NAMES = ["ID",
               "NAME",
               "HotelAdvisorId"]

                        

def select_for_insert_query(last_update):
        return "select " + defs.fields_str(FIELD_NAMES, "MAINROOMTYPE") + " from Admin.MAINROOMTYPE"


def select_for_update_query(last_update):
        return "select " + defs.fields_str(FIELD_NAMES, "MAINROOMTYPE") + " from Admin.MAINROOMTYPE"