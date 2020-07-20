# class ListNode:
#     def __init__(self, data):
#         self.next = None
#         self.data = data
#         self.head = None
#
#     def getData(self):
#         return self.data
#
#     def getNext(self):
#         return self.next
#
#     def setData(self, newdata):
#         self.data = newdata
#
#     def setNext(self, newnext):
#         self.next = newnext
#
#     def reverse(self):
#         prev = None
#         current = self.head
#         while (current is not None):
#             next = current.next
#             current.next = prev
#             prev = current
#             current = next
#         self.head = prev
#
#
# temp = ListNode(93)
# temp.setNext(23)
# temp.setNext(32)
# print temp.getData()
# print temp.getNext()
# print temp.getNext()
# print temp.reverse()
class Node:
    def __init__(self, data):
        self.data = data
        self.next = None


class LinkedList:
    def __init__(self):
        self.head = None

    def add(self, val):
        new_node = Node(val)
        new_node.next = self.head
        self.head = new_node

    def printList(self):
        temp = self.head
        while temp:
            print temp.data,
            temp = temp.next

    def reversing(self):
        prev = None
        current = self.head
        while current is not None:
            next = current.next
            current.next = prev
            prev = current
            current = next
        self.head = prev


a = LinkedList()
a.add(2)
a.add(5)
a.add(22)
a.printList()
a.reversing()
print "\nReversed Linked List"
a.printList()
