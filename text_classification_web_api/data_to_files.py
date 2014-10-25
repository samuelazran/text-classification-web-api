import codecs
from models import mysession, dispose, Datum

dataFolder='D:/projects/machine-learning-projects/text-classification/text_classification/data/corpora/raw/en/bitcoin/twitter/'

def class_name_by_value(class_value):
    return {
        -1:'-1_negative',
        0: '0_informative',
        1: '1_positive',
        }.get(class_value, None)    # None is default if class_value not found

def save(language, source, domain):
    q = mysession.query(Datum).filter(Datum.gold==True, Datum.language==language, Datum.source==source, Datum.domain==domain)
    data = q.all()
    for datum in data:
        print "saving " + str(datum.id)
        filename = str(datum.id)+".txt"
        fullpath = dataFolder + class_name_by_value(datum.class_value) + "/" + filename
        f = codecs.open(fullpath, 'w', 'utf-8')
        f.write(datum.text)
        f.close
        print "done"
    dispose()

if __name__=="__main__":
    save("en","twitter","bitcoin")