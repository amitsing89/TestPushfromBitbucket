import Algorithmia

api_key = "sima/xSiOIZ+UxY9mUBpCvr+TIg1"
input = ["data://home/cloudera/Pictures/test.jpg", "data://home/cloudera/Pictures/mine.jpg"]

client = Algorithmia.client(api_key)

nlp_directory = client.dir("data://amit_singh/nlp_directory")
algo = client.algo('zskurultay/ImageSimilarity/0.1.4')
print algo.pipe(input)
