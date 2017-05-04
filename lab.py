from concepts import Context, Definition
import pandas as pd
import sys

def transform_nominal(dataframe, col_name, col_values):
    for value in col_values:
        dataframe[col_name + '_' + value] = dataframe[col_name].apply(lambda x: 'X' if x == value else None)
    del dataframe[col_name]
    
def transform_counternominal(dataframe, col_name, col_values):
    for value in col_values:
        dataframe[col_name + '_' + value] = dataframe[col_name].apply(lambda x: None if x == value else 'X')
    del dataframe[col_name]
    
def transform_ordinal(dataframe, col_name, col_values):
    dataframe[col_name] = dataframe[col_name].apply(lambda x: col_values.index(x))
    for i in xrange(len(col_values)):
        dataframe[col_name + '_<=' + col_values[i]] = dataframe[col_name].apply(lambda x: 'X' if x <= i else None)
    del dataframe[col_name]
    
def transform_interval(dataframe, col_name, col_values):
    dataframe[col_name] = dataframe[col_name].apply(lambda x: col_values.index(x))
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
        if (i == 0):
            continue
        transform_column(dataframe, col, col_defs[col])

def build_iceberg_lattice(filename, lattice, threshold):
    irreducable = []
    for i, (intent, extent) in enumerate(lattice):
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
    dataframe.to_csv('transformed.' + filename, index_label=False, index=False)

    context = Context.fromfile('transformed.' + filename, frmat='csv')
    lattice_str = str(context.lattice.graphviz())
    f = open('lattice.dot', 'w')
    f.write(lattice_str)
    f.close()
    context.lattice.graphviz()

    build_iceberg_lattice(filename, context.lattice, iceberg_threshold)
    iceberg_context = Context.fromfile(filename='iceberg.' + filename, frmat='csv')
    iceberg_context.lattice.graphviz()

    lattice_str = str(iceberg_context.lattice.graphviz())
    f = open('iceberg.dot', 'w')
    f.write(lattice_str)
    f.close()


if __name__ == "__main__": main()
