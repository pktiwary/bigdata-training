package demo.hbase;

import java.io.IOException;
import java.util.ArrayList;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

/**
 * Servlet implementation class HBaseServlet
 */
public class HBaseServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	final static Logger logger = Logger.getLogger(HBaseServlet.class);
    /**
     * @see HttpServlet#HttpServlet()
     */
    public HBaseServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
        doPost(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		if(request.getParameter("action").equals("Query"))
		{
			QueryHBaseTable hbase = new QueryHBaseTable();
		    ArrayList<String[]> result = hbase.query(request.getParameter("hbaseTableName"),
			    	request.getParameter("columnFamily"),
			    	request.getParameter("columns").split(","));
		    request.setAttribute("result", result);
		}
		else if(request.getParameter("action").equals("Create"))
		{
			AdminHBaseTable hbase = new AdminHBaseTable();
			hbase.create(request.getParameter("hbaseTableName"),
					request.getParameter("columnFamily"));
			request.setAttribute("message", "Table created successfully");
		}
		RequestDispatcher view = request.getRequestDispatcher("result.jsp");
	    view.forward(request,  response);
	}

}
