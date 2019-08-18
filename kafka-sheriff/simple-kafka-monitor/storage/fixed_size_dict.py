from collections import OrderedDict
import json

class FixedSizeDict(OrderedDict):
  def __init__(self, *args, **kwds):
    self.size_limit = kwds.pop("size", None) #size_limit
    OrderedDict.__init__(self, *args, **kwds)
    self._check_size_limit()

  def __setitem__(self, key, value):
    OrderedDict.__setitem__(self, key, value)
    self._check_size_limit()

  def _check_size_limit(self):
    if self.size_limit is not None:
      while int(len(self)) > int(self.size_limit):
        self.popitem(last=False)

  def from_json(self, json_str):
    json_obj = json.loads(json_str)
    for key in json_obj:
      self[key] = json_obj[key]
    return self

  def to_json(self):
    #json_str = {}
    #full_json = []
    #for key in self:
    #  json_str = {key : self[key]}
    #  full_json.append(json_str)
    return json.dumps(self)

  def cast_to_ordered_dict(self):
    return OrderedDict(sorted(self.items()))