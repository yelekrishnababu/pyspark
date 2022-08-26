#pip install pypiwin32
import win32com.client
metadata = ['Name', 'Size', 'Item type', 'Date modified', 'Date created']
folder= 'C:\\Users\\KY2910_krishna\\PycharmProjects\\pyspark\\venv'
filename="test.json"
def get_file_metadata(path,filename,metadata):
    sh = win32com.client.gencache.EnsureDispatch('Shell.Application', 0)
    ns = sh.NameSpace(path)
    file_metadata = dict()
    item = ns.ParseName(str(filename))
    print(item)
    for ind, attribute in enumerate(metadata):
        attr_value = ns.GetDetailsOf(item, ind)
        if attr_value:
            file_metadata[attribute] = attr_value
    return file_metadata
dict=get_file_metadata( folder,filename, metadata)
for k,v in dict.items():
    print(k,v)
# with open("sample.json", "w") as outfile:
#     json.dump(dictionary, outfile)
