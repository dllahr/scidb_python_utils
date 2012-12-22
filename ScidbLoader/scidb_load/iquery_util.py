import commands
    

class IqueryUtil:
    """
    contains utility functions that issue iquery commands through the OS
    and parse / return the results
    also does work with csv2scidb
    """

    def remove_array_if_present(self, db, array_name):
        findArrayCommand = """iquery -o csv -aq "list()" | grep -c '{}' """.format(array_name)
        array_found = commands.getoutput(findArrayCommand)
        print "array_found: {}".format(array_found)
        
        if (array_found == "1"):
            result = db.executeQuery("remove({})".format(array_name))
            db.completeQuery(result.queryID)


    def get_min_and_max(self, attribute_dimension_list, array_name):
        statement = ""
        for attribute in attribute_dimension_list:
            statement += "min(" + attribute + "), max(" + attribute + "), "

        statement = statement[0:(len(statement)-2)]

        statement = "select " + statement + " from " + array_name

        result = commands.getoutput("iquery -q \"" + statement + "\"")
        result = result[2:(len(result)-2)]

        result_num = list()
        for result_str in result.split(","):
            if result_str != "null":
                result_num.append(int(result_str))
            else:
                result_num.append(None)
 
        result_dict = dict()
        i = 0
        for attribute in attribute_dimension_list:
            min_val = result_num[2*i]
            max_val = result_num[2*i + 1]

            result_dict[attribute] = min_val, max_val
            i += 1

        return result_dict


