__author__ = 'Samuel'
import os, imp

class Config(dict):
    def from_pyfile(self, filename, silent=False):
        """
        based on 'from_pyfile' from flask package
        :param filename: the filename of the config.  This can be a filename relative to the
                         root path.
        :param silent: set to `True` if you want silent failure for missing
                       files.

        """

        filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
        d = imp.new_module('config')
        d.__file__ = filename
        try:
            with open(filename) as config_file:
                exec(compile(config_file.read(), filename, 'exec'), d.__dict__)
        except IOError as e:
            if silent:
                return False
            e.strerror = 'Unable to load configuration file (%s)' % e.strerror
            raise
        self.from_object(d)
        return True
    def from_object(self, obj):
        """Updates the values from the given object.  An object can be:

        -   an actual object reference: that object is used directly

        """
        for key in dir(obj):
            if key.isupper():
                self[key] = getattr(obj, key)

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, dict.__repr__(self))


class Bunch(dict):
    """Container object for datasets: dictionary-like object that
       exposes its keys as attributes."""

    def __init__(self, **kwargs):
        dict.__init__(self, kwargs)
        self.__dict__ = self

import re
import htmlentitydefs

class HtmlUtils(object):
    ##
    # Removes HTML or XML character references and entities from a text string.
    #
    # @param text The HTML (or XML) source text.
    # @return The plain text, as a Unicode string, if necessary.
    def unescape(self, text):
        def fixup(m):
            text = m.group(0)
            if text[:2] == "&#":
                # character reference
                try:
                    if text[:3] == "&#x":
                        return unichr(int(text[3:-1], 16))
                    else:
                        return unichr(int(text[2:-1]))
                except ValueError:
                    pass
            else:
                # named entity
                try:
                    text = unichr(htmlentitydefs.name2codepoint[text[1:-1]])
                except KeyError:
                    pass
            return text # leave as is
        return re.sub("&#?\w+;", fixup, text)
