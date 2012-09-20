package edu.ucsc.dbtune.bip.util;

import java.io.File;
import java.io.PrintStream;
import java.util.List;

import edu.ucsc.dbtune.util.Rt;

public class LatexGenerator 
{
    public static class Plot
    {
        String fileName;
        String caption;
        double scale;
        
        public Plot(String _name, String _caption, double _scale)
        {
            fileName = _name;
            caption = _caption;
            scale = _scale;
        }
    }
    
    public static final String latex = "/usr/bin/xelatex";
    
    public static void generateLatex(File latexFile, File outputDir, List<Plot> plots) 
                    throws Exception
    {
        PrintStream ps = new PrintStream(latexFile);
        ps.println("\\documentclass{sig-alternate}\n" + "\n"
                + "\\usepackage{graphicx} % need for figures\n" + "\n"
                + "\\usepackage{subfigure}\n" + "\n"
                + "\\begin{document}\n" + "" + "");
        
        for (Plot plot : plots)
            ps.println("\\begin{figure}\n" + "\\centering\n"
                    + "\\includegraphics[scale=" + plot.scale + "]{" + plot.fileName
                    + ".eps}\n" + "\\caption{" + plot.caption + " }\n"
                    + "\\end{figure}\n" + "");
        
        ps.println("\\end{document}\n");
        ps.close();
        //Rt.runAndShowCommand(latex + " -interaction=nonstopmode " + latexFile, outputDir);
    }
}
