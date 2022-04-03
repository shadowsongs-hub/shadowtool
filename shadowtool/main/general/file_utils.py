from PIL import Image


def is_file_image(file_path: str) -> bool:
    try:
        Image.open(file_path)
        return True
    except IOError:
        return False
