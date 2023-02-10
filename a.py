from mpi4py import MPI


# Set up the MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()    


print("Rank: ", rank, "hey joseph!")