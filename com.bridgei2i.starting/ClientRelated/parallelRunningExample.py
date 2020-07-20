from multiprocessing import Process


def claimParallelism(source, claim_tar):
    for target in claim_tar:
        print (source, target)


claim_source = range(10)
claim_tar = range(500)
if __name__ == '__main__':
    for source in claim_source:
        p = Process(target=claimParallelism, args=(source, claim_tar,))
        p.start()
