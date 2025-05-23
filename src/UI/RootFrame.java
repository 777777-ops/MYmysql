package UI;

import Memory.*;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableColumn;
import java.awt.*;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

public class RootFrame extends JFrame {
    private static RootFrame frame = null;  //单例实现

    private Table table;   //表数据

    private final CardLayout cardLayout = new CardLayout();
    private final JPanel container = new JPanel(cardLayout);         //卡片集合（页集合）
    private final HashSet<String> set = new HashSet<>();            //卡片中的panel

    //界面初始化
    public RootFrame(TableFrame parent) {
        this.table = parent.getTable();

        setTitle("ROOT(逻辑测试)");
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setSize(1200, 400);

        // 添加主面板到框架
        add(container, BorderLayout.CENTER);

        // 默认显示根节点
        showPageNodes(table.getRoot().getPage_offset() + "");

        setVisible(true);

    }

    public RootFrame(Table table) {
        this.table = table;

        setTitle("ROOT(逻辑测试)");
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setSize(1200, 400);

        // 添加主面板到框架
        add(container, BorderLayout.CENTER);

        // 默认显示根节点
        showPageNodes(table.getRoot().getPage_offset() + "");

        setVisible(true);
    }

    //显示某一页的属性  对话窗
    private void showPageProperties(Page page) {
        String panelId = page.getPage_offset() + "";

        // 创建对话框
        JDialog dialog = new JDialog(frame, "对话框面板", true);
        dialog.setSize(300, 200);

        // 页属性展示面板
        JPanel propertyPanel = new JPanel(new GridLayout(0, 4, 10, 10));
        LinkedHashMap<String, Object> map = page.getPage_all();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            propertyPanel.add(new JLabel(entry.getKey() + ": "));
            propertyPanel.add(new JLabel(entry.getValue().toString()));
        }

        //面板添加关闭按钮
        JButton closeButton = new JButton("关闭");
        closeButton.addActionListener(e -> dialog.setVisible(false));
        propertyPanel.add(closeButton);

        dialog.add(propertyPanel);
        dialog.setVisible(true);
    }

    //显示某一页的所有节点
    private void showPageNodes(String page_offset) {
        if (!set.contains(page_offset)) {
            //删除掉所有的缓存
            container.removeAll();
            container.revalidate();
            container.repaint();
            int offset = Integer.parseInt(page_offset);
            Page page = table.deSerializePage(offset);
            createPageNodes(page);
        }
        cardLayout.show(container, page_offset);
    }

    //创建某一页的所有节点
    private void createPageNodes(Page page) {
        //本卡
        JPanel card = new JPanel(new BorderLayout());

        //1.标题
        JLabel titleLabel = new JLabel("LEVEL: " + page.getPage_level() + "      PAGE_OFFSET: " + page.getPage_offset(), SwingConstants.CENTER);
        titleLabel.setFont(new Font("微软雅黑", Font.BOLD, 24));
        card.add(titleLabel, BorderLayout.NORTH);

        //2.子节点表格
        JTable childTable; // 使用数组包装以便在匿名类中修改
        if (page instanceof PageNoLeaf)
            childTable = createChildTableNoLeaf(page);
        else {
            childTable = createChildTableLeaf(page);
        }
        final JScrollPane scrollPane = new JScrollPane(childTable);
        card.add(scrollPane, BorderLayout.CENTER);

        //3.按钮
        JPanel buttonPanel = new JPanel(new GridLayout(2, 2, 1, 1));
        JButton prev = new JButton("上一页");
        JButton next = new JButton("下一页");
        if (page instanceof PageLeaf) {
            int p_offset = ((PageLeaf) page).page_prev_offset;
            int n_offset = ((PageLeaf) page).page_next_offset;
            prev.addActionListener(e -> {
                if (p_offset != 0) showPageNodes(p_offset + "");
                else JOptionPane.showMessageDialog(this, "该页已为首页");
            });
            next.addActionListener(e -> {
                if (n_offset != 0) showPageNodes(n_offset + "");
                else JOptionPane.showMessageDialog(this, "该页已为尾页");
            });
        }
        else {
            prev.addActionListener(e -> JOptionPane.showMessageDialog(this, "非叶子页无上一页"));
            next.addActionListener(e -> JOptionPane.showMessageDialog(this, "非叶子页无上一页"));
        }
        //详细界面
        JButton show = new JButton("详细");
        show.addActionListener(e -> showPageProperties(page));
        buttonPanel.add(show);
        //返回根页
        JButton root = new JButton("根页");
        root.addActionListener(e -> showPageNodes(table.getRoot().getPage_offset() + ""));
        buttonPanel.add(root);
        //刷新
        JButton reNew = new JButton("刷新");
        reNew.addActionListener(e -> {
            // 刷新表格数据
            JTable newTable;
            if (page instanceof PageNoLeaf) {
                newTable = createChildTableNoLeaf(page);
            } else {
                newTable = createChildTableLeaf(page);
            }
            //滚动面板  更新
            scrollPane.setViewportView(newTable);
        });
        buttonPanel.add(reNew);

        buttonPanel.add(prev);
        buttonPanel.add(next);
        card.add(buttonPanel, BorderLayout.SOUTH);

        //添加到卡片集合里
        container.add(card, page.getPage_offset() + "");
        //set.add(page.getPage_offset() + "");



    }

    // 创建子节点表格(非叶子页)
    private JTable createChildTableNoLeaf(Page page){
        String[] columnNames = page.getNodeProperties();
        DefaultTableModel model = new DefaultTableModel(columnNames, 0) {
            @Override
            public boolean isCellEditable(int row, int column) {
                return column == 7; // 只有操作列可编辑
            }
        };

        //将所有数据读取至表上
        Object[][] data = page.getAllNodeData();
        for (Object[] objects : data)
            model.addRow(objects);

        JTable jTable = new JTable(model);
        TableColumn buttonColumn = jTable.getColumnModel().getColumn(7);
        buttonColumn.setCellRenderer(new ButtonRenderer());
        buttonColumn.setCellEditor(new ButtonEditor(new JCheckBox(),jTable));

        return jTable;
    }

    // 创建子节点表格(叶子页)
    private JTable createChildTableLeaf(Page page) {
        String[] columnNames = page.getNodeProperties();
        DefaultTableModel model = new DefaultTableModel(columnNames, 0);
        //将所有数据读取至表上
        Object[][] data = page.getAllNodeData();
        for (Object[] objects : data)
            model.addRow(objects);

        return new JTable(model);
    }

    // 表格按钮渲染器
    class ButtonRenderer extends JButton implements javax.swing.table.TableCellRenderer {
        public ButtonRenderer() {
            setOpaque(true);
        }

        public Component getTableCellRendererComponent(JTable table, Object value,
                                                       boolean isSelected, boolean hasFocus, int row, int column) {
            setText((value == null) ? "" : value.toString());
            return this;
        }
    }

    // 表格按钮编辑器
    class ButtonEditor extends DefaultCellEditor {
        private String label;
        private JTable jTable;

        public ButtonEditor(JCheckBox checkBox, JTable jTable) {
            super(checkBox);
            this.jTable = jTable;
            setClickCountToStart(1);
        }

        public Component getTableCellEditorComponent(JTable table, Object value,
                                                     boolean isSelected, int row, int column) {
            label = (value == null) ? "" : value.toString();
            JButton button = new JButton(label);
            button.addActionListener(e -> {
                // 获取对应子节点并跳转
                String page_offset = jTable.getModel().getValueAt(row, 7).toString();
                showPageNodes(page_offset);
                fireEditingStopped();
            });
            return button;
        }

        public Object getCellEditorValue() {
            return label;
        }
    }

    //单例实现
    public static RootFrame getInstance(TableFrame parent) {
        if (frame == null || !frame.isDisplayable()) {
            frame = new RootFrame(parent);
        }
        return frame;
    }


}