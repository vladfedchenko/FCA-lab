from concepts import Context, Definition
import pandas as pd
import sys
import itertools as itt

def transform_nominal(dataframe, col_name, col_values):
    for value in col_values:
        dataframe[col_name + '_' + value] = dataframe[col_name].apply(lambda x: 'X' if str(x) == value else None)
    del dataframe[col_name]
    
def transform_counternominal(dataframe, col_name, col_values):
    for value in col_values:
        dataframe[col_name + '_' + value] = dataframe[col_name].apply(lambda x: None if str(x) == value else 'X')
    del dataframe[col_name]
    
def transform_ordinal(dataframe, col_name, col_values):
    dataframe[col_name] = dataframe[col_name].apply(lambda x: col_values.index(str(x)))
    for i in xrange(len(col_values)):
        dataframe[col_name + '_<=' + col_values[i]] = dataframe[col_name].apply(lambda x: 'X' if x <= i else None)
    del dataframe[col_name]
    
def transform_interval(dataframe, col_name, col_values):
    dataframe[col_name] = dataframe[col_name].apply(lambda x: col_values.index(str(x)))
    for i in xrange(len(col_values)):
        dataframe[col_name + '_<=' + col_values[i]] = dataframe[col_name].apply(lambda x: 'X' if x <= i else None)
    for i in xrange(len(col_values)):
        dataframe[col_name + '_>=' + col_values[i]] = dataframe[col_name].apply(lambda x: 'X' if x >= i else None)
    del dataframe[col_name]
    
def transform_column(dataframe, col_name, col_type):
    if (col_type[0] == 'Nominal'):
        transform_nominal(dataframe, col_name, col_type[1].split(';'))
    elif (col_type[0] == 'Counternominal'):
        transform_counternominal(dataframe, col_name, col_type[1].split(';'))
    elif (col_type[0] == 'Ordinal'):
        transform_ordinal(dataframe, col_name, col_type[1].split(';'))
    elif (col_type[0] == 'Interval'):
        transform_interval(dataframe, col_name, col_type[1].split(';'))

def transform_columns(dataframe, col_defs):
    dataframe.columns = ([''] + list(dataframe.columns[1:]))
    for i, col in enumerate(dataframe.columns):
        if ((i == 0) or not (col in col_defs)):
            continue
        transform_column(dataframe, col, col_defs[col])

def build_iceberg_lattice(filename, lattice, threshold):
    irreducable = []
    for i, (intent, extent) in enumerate(lattice):
        coverage = list(intent)
        if (len(intent) < threshold):
            continue
        is_irreducable = True
        for j, (intent1, extent1) in enumerate(lattice):
            if (j == i or len(intent1) < threshold or len(intent) <= len(intent1)):
                continue
            is_subset = True
            for obj in intent1:
                if (not(obj in intent)):
                    is_subset = False
                    break
            if is_subset:
                for obj in intent1:
                    if obj in coverage:
                        coverage.remove(obj)
                if (len(coverage) == 0):
                    is_irreducable = False
                    break
        if is_irreducable:
            irreducable.append((intent, extent))
            #print intent, extent
            #print '\n'
    df = Definition()
    for intent, extent in irreducable:
        obj_name = ';'.join(intent)
        df.add_object(obj_name, list(extent))
    conc = Context(*df)
    conc.tofile(filename='iceberg.' + filename, frmat='csv')

def is_sublist(parent, child):
    for el in child:
        if not(el in parent):
            return False
    return True
        
def find_implication_basis(cont):
    pseudointents = []
    props = list(cont.properties)
    j = 1
    for i in xrange(1, len(props)):
        for subset in itt.combinations(props, i):
            #Checking first pseudointent condition
            subset = list(subset)
            #print str(j) + " " + str(subset)
            j += 1
            extension = list(cont.extension(subset))
            intension = list(cont.intension(extension))
            if (len(subset) >= len(intension)):
                #print "Rejected first: " + str((subset, extension))
                continue
                
            #checking second pseudointent condition
            second_passed = True
            for conf_intent, conf_extent in pseudointents:
                if (is_sublist(subset, conf_intent) and (not is_sublist(subset, conf_extent))):
                    #print "Rejected second: " + str((subset, extension))
                    second_passed = False
            if second_passed:
                pseudointents.append((subset, intension))
    return pseudointents

def main():
    #print sys.argv
    filename = str(sys.argv[1])
    iceberg_threshold = int(sys.argv[2])
    draw_iceberg = False
    cols_to_use= []
    i = 3
    cols_started = False
    while (i < len(sys.argv)):
	if (sys.argv[i][0] == '-'):
	    cols_started = False
	    if (sys.argv[i] == '-draw'):
		draw_iceberg = True
	    elif (sys.argv[i] == '-columns'):
		cols_started = True
	elif (cols_started):
	    cols_to_use.append(sys.argv[i])
	i += 1
    #print cols_to_use

    dataframe = pd.read_csv(filename)
    if (len(cols_to_use) > 0):
	dataframe = dataframe[[dataframe.columns[0]] + cols_to_use]
    col_info = pd.read_csv('cols.' + filename)
    transform_columns(dataframe, col_info)
    dataframe = dataframe.drop_duplicates(subset=list(dataframe.columns[0:1]), keep='first')
    dataframe.to_csv('transformed.' + filename, index_label=False, index=False)

    context = Context.fromfile('transformed.' + filename, frmat='csv')
    lattice_str = str(context.lattice.graphviz())
    f = open('lattice.dot', 'w')
    f.write(lattice_str)
    f.close()
    #context.lattice.graphviz()

    build_iceberg_lattice(filename, context.lattice, iceberg_threshold)
    iceberg_context = Context.fromfile(filename='iceberg.' + filename, frmat='csv')
    if draw_iceberg:
   	iceberg_context.lattice.graphviz(view=True)

    lattice_str = str(iceberg_context.lattice.graphviz())
    f = open('iceberg.dot', 'w')
    f.write(lattice_str)
    f.close()

    implication_basis = find_implication_basis(iceberg_context)
    print "Implication basis: "
    for i, e in implication_basis:
	print str(i) + " => " + str(e)

if __name__ == "__main__": main()
