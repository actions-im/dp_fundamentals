package fundamentals.rdds;

import java.io.File;

public class FilePartition extends Partition{

    int partitionId;
    File file;

    @Override
    public int getPartitionId() {
        return super.getPartitionId();
    }

    public File getPartitionFile(){
        return file;
    }

    public FilePartition(int partitionId, File file) {
        this.partitionId = partitionId;
        this.file = file;
    }
}
