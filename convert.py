from mod_python import apache

pinyin = apache.import_module('src/pinyin')
converter = pinyin.Converter()

def index(text, fmt = 'df', sc = 'true', pp = 'false', fuzzy = '0'):

    if sc == 'true':
        sc = True
    elif sc == 'false':
        sc = False
    else:
        sc = True

    if pp == 'true':
        pp = True
    elif pp == 'false':
        pp = False
    else:
        pp = False

    try:
        fuzzy = int(fuzzy)
    except ValueError:
        fuzzy = 0

    global converter
    return converter.convert(text, fmt, sc, pp, fuzzy)