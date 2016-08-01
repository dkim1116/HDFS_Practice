from mrjob.job import MRJob

class MRFriendsByAge(MRJob):

    def mapper(self, _, line):
        (ID, name, age, numFriends) = line.split(',') #split data by commas as the file is csv
        yield age, float(numFriends) #type cast the number value

    def reducer(self, age, numFriends):
        total = 0
        numElements = 0
        for x in numFriends:
            total += x
            numElements += 1
            
        yield age, total / numElements


if __name__ == '__main__':
    MRFriendsByAge.run()