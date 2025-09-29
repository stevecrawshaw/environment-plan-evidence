
pacman::p_load(tidyverse, emojifont, ggtext, patchwork, showtext)

# Load the font file. The first argument "fontawesome" is the family name we will use in ggplot.
font_add(
  family = "fontawesome", 
  regular = "font-awesome-4.7.0/fonts/fontawesome-webfont.ttf"
)
# Enable showtext to automatically render text in plots
showtext_auto()
# Create the data frame
# --- THE ONLY CHANGE IS THE LAST ICON CODE ---
df <- data.frame(
  category = c("Domestic", "Industry & Commercial", "Transport"),
  value = c(39, 32, 29),
  icon = c("\uf015", "\uf275", "\uf1b9") # Home, Industry, Car
)

# Calculate positions for labels and icons
df <- df %>%
  # Arrange data to match the visual layout (clockwise from the top)
  arrange(match(category, c("Industry & Commercial", "Transport", "Domestic"))) %>%
  mutate(
    y_pos = cumsum(value) - 0.5 * value,
    label_text = paste0(value, "%")
  )


# Define colors to match the image
bg_color <- "#1A2A3A"
line_color <- "#A8B820"

# Create the pie chart plot
pie_chart <- ggplot(df, aes(x = 2, y = value)) +
  # Create the pie slices. We use a dark fill to match the background
  # and a colored border to create the "ring" effect.
  geom_col(width = 1.8, color = line_color, fill = bg_color, linewidth = 0.5) +

  # Add the percentage labels outside the circle
  geom_text(aes(x = 3.1, y = y_pos, label = label_text),
            color = line_color,
            fontface = "bold",
            size = 6) +

  # Add the icons using geom_text with the Font Awesome family
  geom_text(aes(x = 2, y = y_pos, label = icon),
            family = "fontawesome", # Use Font Awesome font
            size = 15,
            color = line_color) +

  # Convert the bar chart to a pie chart
  coord_polar(theta = "y", start = pi/2, direction = -1) +

  # Set limits to create space around the pie
  xlim(c(0.5, 3.2)) +

  # Use a minimal theme and set the background color
  theme_void() +
  theme(
    plot.background = element_rect(fill = bg_color, color = NA),
    panel.background = element_rect(fill = bg_color, color = NA)
  )

# Define the text content with HTML for styling
text_content <- "
<span style='font-size:24pt; color:white; font-weight:bold;'>Energy Use</span><br><br>
<span style='font-size:18pt; color:white;'>by End User</span><br><br><br>
<span style='font-size:14pt; color:white;'>Energy consumption<br> is split evenly<br>between: </span><br><br>
<b style='font-size:22pt; color:#A8B820;'>29%</b> <span style='font-size:14pt; color:white;'> Transport</span><br>
<b style='font-size:22pt; color:#A8B820;'>39%</b> <span style='font-size:14pt; color:white;'> Domestic</span><br>
<b style='font-size:22pt; color:#A8B820;'>32%</b> <span style='font-size:14pt; color:white;'> Industry & Commercial</span>"

# Create the text panel plot
text_panel <- ggplot() +
  annotate(
    "richtext", x = 0, y = 0.5,
    label = text_content,
    hjust = 0, vjust = 0.5, # Align text to the left and center vertically
    label.color = NA, fill = NA # No box around the text
  ) +
  theme_void() +
  theme(plot.background = element_rect(fill = bg_color, color = NA))

# Combine the two plots side-by-side
final_plot <- text_panel + pie_chart +
  plot_layout(widths = c(1, 1.2)) # Give the pie chart slightly more space

# Display the final plot
print(final_plot)
