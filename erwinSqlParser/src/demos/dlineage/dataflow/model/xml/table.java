
package demos.dlineage.dataflow.model.xml;

import demos.dlineage.util.Pair;
import java.util.List;
import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.ElementList;

public class table
{

	@Attribute(required = false)
	private String name;

	@Attribute(required = false)
	private String id;

	@Attribute(required = false)
	private String type;

	@Attribute(required = false)
	private String coordinate;

	@Attribute(required = false)
	private String alias;

	@Attribute(required = false)
	private String isTarget;

	@ElementList(entry = "column", inline = true, required = false)
	private List<column> columns;

	public String getAlias( )
	{
		return alias;
	}

	public void setAlias( String alias )
	{
		this.alias = alias;
	}

	public List<column> getColumns( )
	{
		return columns;
	}

	public void setColumns( List<column> columns )
	{
		this.columns = columns;
	}

	public String getCoordinate( )
	{
		return coordinate;
	}

	public void setCoordinate( String coordinate )
	{
		this.coordinate = coordinate;
	}

	public String getName( )
	{
		return name;
	}

	public void setName( String name )
	{
		this.name = name;
	}

	public String getId( )
	{
		return id;
	}

	public void setId( String id )
	{
		this.id = id;
	}

	public String getType( )
	{
		return type;
	}

	public void setType( String type )
	{
		this.type = type;
	}

	public boolean isView( )
	{
		return "view".equals( type );
	}

	public boolean isTable( )
	{
		return "table".equals( type );
	}

	public boolean isResultSet( )
	{
		return type != null && !isView( ) && !isTable( );
	}

	public Pair<Integer, Integer> getStartPos( )
	{
		return PositionUtil.getStartPos( coordinate );
	}

	public Pair<Integer, Integer> getEndPos( )
	{
		return PositionUtil.getEndPos( coordinate );
	}

	public boolean isTarget( )
	{
		return "true".equals( isTarget );
	}

}
