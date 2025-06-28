import yaml


def get_platforms():
    with open("config.yml", "r") as file:
        config = yaml.safe_load(file)
    return config.get("platforms", [])


def get_people():
    """Return the people dictionary from the config file."""

    with open("config.yml", "r") as file:
        config = yaml.safe_load(file)
    return config.get("people", {})


def get_person(slug):
    """Return a single person's info by their slug key."""

    return get_people().get(slug)
