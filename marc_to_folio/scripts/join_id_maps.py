import argparse
import json
from os import listdir
from os.path import isfile, join


def main():
    '''Main method. Magic starts here.'''
    parser = argparse.ArgumentParser()
    parser.add_argument("data_path", help="path to folder with files to map")
    parser.add_argument("result_path", help="path to resutls file")
    args = parser.parse_args()
    my_map = dict()
    print("\tFolder with files:\t", args.data_path)
    print('\tresults file:\t', args.result_path)
    files = [f for f in listdir(args.data_path)
             if isfile(join(args.data_path, f))]
    print(f"Files to join:{len(files)}")
    for file_name in files:
        with open(join(args.data_path, file_name), 'r') as file:
            temp_map = json.load(file)
            print(f"{len(temp_map)} items in map")
            my_map.update(temp_map)
            print(f"{len(my_map)} items in joint map")
    with open(args.result_path, 'w+') as res_file:
        res_file.write(json.dumps(my_map))
    print("Done!")


if __name__ == '__main__':
    main()
