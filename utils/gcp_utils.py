def make_gcs_path(*args: str) -> str:
    if args:
        return "gs://" + "/".join(args)
    else:
        return ""