# class BinaryTree():
#     def __init__(self, rootid):
#         self.left = None
#         self.right = None
#         self.rootid = rootid
#
#     def getLeftChild(self):
#         return self.left
#
#     def getRightChild(self):
#         return self.right
#
#     def setNodeValue(self, value):
#         self.rootid = value
#
#     def getNodeValue(self):
#         return self.rootid
#
#     def insertRight(self, newNode):
#         if self.right is None:
#             self.right = BinaryTree(newNode)
#         else:
#             tree = BinaryTree(newNode)
#             tree.right = self.right
#             self.right = tree
#
#     def insertLeft(self, newNode):
#         if self.left is None:
#             self.left = BinaryTree(newNode)
#         else:
#             tree = BinaryTree(newNode)
#             tree.left = self.left
#             self.left = tree
#
#
# def printTree(tree):
#     if tree is not None:
#         printTree(tree.getLeftChild())
#         print(tree.getNodeValue())
#         printTree(tree.getRightChild())
#
#
# def testTree():
#     myTree = BinaryTree("Maud")
#     myTree.insertLeft("Bob")
#     myTree.insertRight("Tony")
#     myTree.insertRight("Steven")
#     myTree.insertLeft("Amit")
#     printTree(myTree)
#
#
# testTree()

# class Node:
#     def __init__(self, val):
#         self.l = None
#         self.r = None
#         self.v = val
# 
# 
# class Tree:
#     def __init__(self):
#         self.root = None
# 
#     def getRoot(self):
#         return self.root
# 
#     def add(self, val):
#         if self.root is None:
#             self.root = Node(val)
#         else:
#             self._add(val, self.root)
# 
#     def _add(self, val, node):
#         if val < node.v:
#             if node.l is not None:
#                 self._add(val, node.l)
#             else:
#                 node.l = Node(val)
#         else:
#             if node.r is not None:
#                 self._add(val, node.r)
#             else:
#                 node.r = Node(val)
# 
#     def find(self, val):
#         if self.root is not None:
#             return self._find(val, self.root)
#         else:
#             return None
# 
#     def _find(self, val, node):
#         if val == node.v:
#             return node
#         elif val < node.v and node.l is not None:
#             self._find(val, node.l)
#         elif val > node.v and node.r is not None:
#             self._find(val, node.r)
# 
#     def deleteTree(self):
#         # garbage collector will do this for us.
#         self.root = None
# 
#     def printTree(self):
#         if self.root is not None:
#             self._printTree(self.root)
# 
#     def _printTree(self, node):
#         if node is not None:
#             self._printTree(node.l)
#             print str(node.v) + ' '
#             self._printTree(node.r)
# 
# 
# # 3
# # 0     4
# #   2      8
# tree = Tree()
# tree.add(3)
# tree.add(4)
# tree.add(0)
# tree.add(8)
# tree.add(2)
# tree.printTree()
# print "T", tree.find(3).v
# print "T", tree.find(8)
# tree.deleteTree()
# tree.printTree()
