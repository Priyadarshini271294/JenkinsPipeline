
package demos.dlineage.dataflow.model;

import demos.dlineage.util.Pair;
import gudusoft.gsqlparser.TSourceToken;
import gudusoft.gsqlparser.nodes.TObjectName;
import java.util.ArrayList;
import java.util.List;

public class ViewColumn
{

	private View view;

	private int id;
	private String name;

	private Pair<Long, Long> startPosition;
	private Pair<Long, Long> endPosition;

	private TObjectName columnObject;
	private List<TObjectName> starLinkColumns = new ArrayList<TObjectName>( );

	private int columnIndex;

	public ViewColumn( View view, TObjectName columnObject, int index )
	{
		if ( view == null || columnObject == null )
			throw new IllegalArgumentException( "TableColumn arguments can't be null." );

		id = ++TableColumn.TABLE_COLUMN_ID;

		this.columnObject = columnObject;

		TSourceToken startToken = columnObject.getStartToken( );
		TSourceToken endToken = columnObject.getEndToken( );
		this.startPosition = new Pair<Long, Long>( startToken.lineNo,
				startToken.columnNo );
		this.endPosition = new Pair<Long, Long>( endToken.lineNo,
				endToken.columnNo + endToken.astext.length( ) );

		if ( !"".equals( columnObject.getColumnNameOnly( ) ) )
			this.name = columnObject.getColumnNameOnly( );
		else
			this.name = columnObject.toString( );

		this.view = view;
		this.columnIndex = index;
		view.addColumn( this );
	}

	public View getView( )
	{
		return view;
	}

	public int getId( )
	{
		return id;
	}

	public String getName( )
	{
		return name;
	}

	public Pair<Long, Long> getStartPosition( )
	{
		return startPosition;
	}

	public Pair<Long, Long> getEndPosition( )
	{
		return endPosition;
	}

	public TObjectName getColumnObject( )
	{
		return columnObject;
	}

	public void bindStarLinkColumns( List<TObjectName> starLinkColumns )
	{
		if ( starLinkColumns != null && !starLinkColumns.isEmpty( ) )
		{
			this.starLinkColumns = starLinkColumns;
		}
	}

	public List<TObjectName> getStarLinkColumns( )
	{
		return starLinkColumns;
	}

	public int getColumnIndex( )
	{
		return columnIndex;
	}

}
