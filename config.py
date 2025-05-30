import yaml


def get_platforms():
    with open("config.yml", "r") as file:
        config = yaml.safe_load(file)
    return config.get("platforms", [])
