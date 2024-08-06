#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from PIL import Image
from io import BytesIO
import base64


def crop_to_box(im, box):
    left, bot, right, top = box
    imgWidth, imgHeight = im.size

    left_prime = int(left * imgWidth)
    right_prime = int(right * imgWidth)

    top_prime = int(top * imgHeight)
    bot_prime = int(bot * imgHeight)

    cropped = im.crop(box=(left_prime, bot_prime, right_prime, top_prime))
    return cropped


def im2bytes(im, format_='jpeg'):
    fp = BytesIO()
    im.save(fp, format=format_)
    return fp.getvalue()


def bytes2im(data):
    im = Image.open(BytesIO(data))
    return im


def encode_image(data, size=None):
    data = base64.b64encode(data).decode("utf-8")
    return data


def decode_image(data):
    data = base64.b64decode(data.encode("utf-8"))
    return data


def resize(data, size=(256, 256)):
    im = Image.open(BytesIO(data))
    im = im.resize((256, 256))
    fp = BytesIO()
    im.save(fp, format='jpeg')
    data = fp.getvalue()
    return data
