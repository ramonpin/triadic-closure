package pin.ramon.hadoop.grafos

import org.apache.hadoop.io.ByteWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.WritableComparable
import java.io.DataInput
import java.io.DataOutput

/**
 * NodeWritable
 * Created by ramon.pin on 9/02/17.
 */
class NodeWritable(var tipo: Byte, var codigo: Int) : WritableComparable<NodeWritable> {

    private var tipoWritable   = ByteWritable(tipo)
    private var codigoWritable = IntWritable(codigo)

    constructor(): this(0, 0)
    constructor(tipo: String, codigo: String) : this(tipo.toByte(), codigo.toInt())
    constructor(other: NodeWritable) : this(other.tipo, other.codigo)


    companion object {

        fun fromString(string: String): NodeWritable {
            val data = string.split("-")
            return NodeWritable(data[0], data[1])
        }

    }

    override fun readFields(input: DataInput?) {
        tipoWritable.readFields(input)
        codigoWritable.readFields(input)

        tipo   = tipoWritable.get()
        codigo = codigoWritable.get()


    }

    override fun write(output: DataOutput) {
        tipoWritable.write(output)
        codigoWritable.write(output)

    }

    override operator fun compareTo(other: NodeWritable): Int {
        val res = (codigo - other.codigo)
        return if(res == 0) (tipo - other.tipo) else res
    }

    override fun toString() : String {
        return tipo.toString() + "-" + codigo.toString()
    }

}

