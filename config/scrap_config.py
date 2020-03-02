from schema import Schema, And, Use, Optional, SchemaError

REPLACES        = [[' ', '%20']]
NOHTTPINIT      = "//"
DEFAULT_TYPES   = {
    "img": {
        "get": ['png', 'jpg', 'tif', 'gif', 'jpeg'],
        "ignore": ['svg', 'icons', 'icon', 'thumbnail'],
        "size_limit": {"yes": True, "size": 40*1024}
    },
    "audio": {
        "get": ['mp3'],
        "ignore": [],
        "size_limit": {"yes": True, "size": 100*1024}
    },
    "video":{
        "get": ['mp4'],
        "ignore": [],
        "size_limit": {"yes": True, "size": 400*1024}
    }
}

DEFAULT_SCHEMA = Schema({
    And(Use(str)): {
        "get": And(Use(list)),
        "ignore": And(Use(list)),
        "size_limit": {"yes": And(Use(bool)), "size": And(Use(int))}
    },
    Optional(And(Use(str))): {
        "get": And(Use(list)),
        "ignore": And(Use(list)),
        "size_limit": {"yes": And(Use(bool)), "size": And(Use(int))}
    }
})