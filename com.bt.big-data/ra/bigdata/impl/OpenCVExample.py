import cv2
import sys
import numpy

# Get user supplied values
imagePath = sys.argv[1]
# cascPath = "C:\work_office\hadoop\learning\Python\facedetection\haarcascade_frontalface_default.xml"
# cascPath="C:\work_office\hadoop\learning\Python\facedetection\xml\haarcascade_frontalface_default.xml"
# cascPath = "C:\\work_officehadoop\\learning\\Python\\facedetection\\haarcascade_frontalface_default.xml"
cascPath = "/root/check/IdeaProjects/pythonscripts/com.bt.big-data/ra/bigdata/impl/haarcascade_frontalface_default.xml"
# Create the haar cascade
# faceCascade = cv2.CascadeClassifier("C:\work_office\hadoop\learning\Python\facedetection\haarcascade_frontalface_default.xml")
faceCascade = cv2.CascadeClassifier(
    "/root/check/IdeaProjects/pythonscripts/com.bt.big-data/ra/bigdata/impl/haarcascade_frontalface_default.xml")
# (cascPath)
# test=faceCascade.load('C:\work_office\hadoop\learning\Python\facedetection\haarcascade_frontalface_default.xml')
test = faceCascade.load(
    "/root/check/IdeaProjects/pythonscripts/com.bt.big-data/ra/bigdata/impl/haarcascade_frontalface_default.xml")
# ('haarcascade_frontalface_default.xml')

# Read the image
image = cv2.imread(imagePath)
# imr = cv2.imwrite('compress_img1.png', image,  [cv2.IMWRITE_PNG_COMPRESSION, 9])
gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

# Detect faces in the image

faces = faceCascade.detectMultiScale(
    gray,
    scaleFactor=1.1,
    minNeighbors=7,
    minSize=(30, 30),
    flags=cv2.CASCADE_SCALE_IMAGE
    # flags = cv2.cv.CV_HAAR_SCALE_IMAGE
    # flags = cv2.CV_HAAR_SCALE_IMAGE
    # flags = 0
)

print("Found {0} faces!".format(len(faces)))

# Draw a rectangle around the faces
for (x, y, w, h) in faces:
    cv2.rectangle(image, (x, y), (x + w, y + h), (0, 255, 0), 2)

cv2.imshow("Faces found", image)
cv2.waitKey(6000)
