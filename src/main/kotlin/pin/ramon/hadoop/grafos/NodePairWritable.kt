package pin.ramon.hadoop.grafos

import org.apache.hadoop.io.WritableComparable
import java.io.DataInput
import java.io.DataOutput

/**
 * NodePairWritable
 * Created by ramon.pin on 14/02/17.
 */
class NodePairWritable(val node1: NodeWritable, val node2: NodeWritable) : WritableComparable<NodePairWritable> {

    constructor() : this(NodeWritable(), NodeWritable())

    companion object {

        fun fromString(string: String) : NodePairWritable {
            val data  = string.split("\t")
            val node1 = NodeWritable.fromString(data[0])
            val node2 = NodeWritable.fromString(data[1])
            return NodePairWritable(node1, node2)
        }

    }

    override fun write(output: DataOutput) {
        node1.write(output)
        node2.write(output)
    }

    override fun readFields(input: DataInput) {
        node1.readFields(input)
        node2.readFields(input)
    }

    override fun compareTo(other: NodePairWritable?): Int {
        if(other != null) {
            val first = node1.compareTo(other.node1)
            return if (first != 0) first else node2.compareTo(other.node2)
        } else {
            return 1
        }
    }

}
