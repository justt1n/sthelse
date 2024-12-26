import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

# Wave parameters
wave_speed = 1.0  # Speed of wave propagation
wave_amplitude = 1.0  # Maximum amplitude of waves
wave_length = 10.0  # Wavelength

time_step = 0.1  # Time step for animation

# Initialize the wave sources
sources = []  # Each source is a tuple (x, y, t_start)

# Grid for visualization
x = np.linspace(-50, 50, 500)
y = np.linspace(-50, 50, 500)
x_grid, y_grid = np.meshgrid(x, y)

# Figure setup
fig, ax = plt.subplots(figsize=(6, 6))
ax.set_xlim(-50, 50)
ax.set_ylim(-50, 50)
ax.set_aspect('equal')
wave_field = ax.imshow(np.zeros((500, 500)), extent=[-50, 50, -50, 50], origin='lower', cmap='plasma', vmin=-2, vmax=2)
ax.set_title("Click to create circular waves")
ax.set_xlabel("X Position")
ax.set_ylabel("Y Position")

# Event handler for mouse clicks
def on_click(event):
    if event.inaxes != ax:
        return
    sources.append((event.xdata, event.ydata, 0))

# Function to calculate wave interference
def calculate_wave_interference(t):
    wave_field = np.zeros_like(x_grid)
    for (x_src, y_src, t_start) in sources:
        r = np.sqrt((x_grid - x_src)**2 + (y_grid - y_src)**2)  # Distance from source
        phase = 2 * np.pi * (r / wave_length - wave_speed * (t - t_start))
        wave_field += wave_amplitude * np.sin(phase)
    return np.sin(wave_field)  # Add a sine modulation for visual effect

# Update function for animation
def update(frame):
    t = frame * time_step
    field = calculate_wave_interference(t)
    wave_field.set_data(field)
    return wave_field,

# Connect click event
fig.canvas.mpl_connect('button_press_event', on_click)

# Animate
ani = FuncAnimation(fig, update, frames=200, interval=50, blit=True)
plt.show()
