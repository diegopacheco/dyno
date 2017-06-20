package com.netflix.dyno.jedis;

/**
 * Describe a REDIS operation that need to be executed on the Pipeline.
 * 
 * @author diegopacheco
 *
 */
public class OperationMetadata {

	private OpName name;
	private Object[] args;

	public OperationMetadata() {}
	
	public OperationMetadata(OpName name, Object[] args) {
		super();
		this.name = name;
		this.args = args;
	}

	public OpName getName() {
		return name;
	}

	public void setName(OpName name) {
		this.name = name;
	}

	public Object[] getArgs() {
		return args;
	}

	public void setArgs(Object[] args) {
		this.args = args;
	}
	
	@SuppressWarnings("rawtypes")
	public Class[] toClassArraySignature(){
		Class[] result = new Class[args.length];
		for(int i = 0; i <= args.length -1; i++)
			result[i] = args[i].getClass();
		return result;
	}

	@Override
	public String toString() {
		return "OperationMetadata [name=" + name + ", args= " + args + "]";
	}

}
